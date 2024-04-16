package sqlcache

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/rancher/steve/pkg/stores/partition/listprocessor"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

// ListOptionIndexer extends Indexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*Indexer

	indexedFields []string
	addField      *sql.Stmt
}

// NewListOptionIndexer returns a SQLite-backed cache.Indexer of unstructured.Unstructured Kubernetes resources of a certain GVK
// ListOptionIndexer is also able to satisfy ListOption queries on indexed (sub)fields
// Fields are specified as slices (eg. "metadata.resourceVersion" is ["metadata", "resourceVersion"])
func NewListOptionIndexer(example *unstructured.Unstructured, keyFunc cache.KeyFunc, fields [][]string, name string, path string) (*ListOptionIndexer, error) {
	// necessary in order to gob/ungob unstructured.Unstructured objects
	gob.Register(map[string]interface{}{})

	v, err := NewIndexer(example, keyFunc, name, path, cache.Indexers{})
	if err != nil {
		return nil, err
	}

	indexedFields := []string{"metadata.name", "metadata.namespace"}
	for _, f := range fields {
		indexedFields = append(indexedFields, toColumnName(f))
	}

	l := &ListOptionIndexer{
		Indexer:       v,
		indexedFields: indexedFields,
	}
	l.RegisterAfterUpsert(l.AfterUpsert)

	columnDefs := []string{}
	for _, field := range indexedFields {
		column := fmt.Sprintf(`"%s" VARCHAR`, field)
		columnDefs = append(columnDefs, column)
	}

	err = l.InitExec(fmt.Sprintf(`CREATE TABLE "%s_fields" (
			key VARCHAR NOT NULL PRIMARY KEY,
            %s
	   )`, v.name, strings.Join(columnDefs, ", ")))
	if err != nil {
		return nil, err
	}

	for _, field := range indexedFields {
		err = l.InitExec(fmt.Sprintf(`CREATE INDEX "%s_%s_index" ON "%s_fields"("%s")`, v.name, field, v.name, field))
		if err != nil {
			return nil, err
		}
	}

	columns := []string{}
	for _, field := range indexedFields {
		column := fmt.Sprintf(`"%s"`, field)
		columns = append(columns, column)
	}

	qmarks := []string{}
	for _ = range indexedFields {
		qmarks = append(qmarks, "?")
	}

	setStatements := []string{}
	for _, field := range indexedFields {
		setStatement := fmt.Sprintf(`"%s" = excluded."%s"`, field, field)
		setStatements = append(setStatements, setStatement)
	}

	l.addField = l.Prepare(fmt.Sprintf(
		`INSERT INTO "%s_fields"(key, %s) VALUES (?, %s) ON CONFLICT DO UPDATE SET %s`,
		v.name,
		strings.Join(columns, ", "),
		strings.Join(qmarks, ", "),
		strings.Join(setStatements, ", "),
	))

	return l, nil
}

/* Core methods */

// AfterUpsert saves sortable/filterable fields into tables
func (l *ListOptionIndexer) AfterUpsert(key string, obj any, tx *sql.Tx) error {
	args := []any{key}
	for _, field := range l.indexedFields {
		value, err := getField(obj, field)
		if err != nil {
			return err
		}
		switch typedValue := value.(type) {
		case nil:
			args = append(args, "")
		case int, bool, string:
			args = append(args, fmt.Sprint(typedValue))
		case []string:
			args = append(args, strings.Join(typedValue, "|"))
		default:
			return errors.Errorf("%v has a non-supported type value: %v", field, value)
		}
		if err != nil {
			return err
		}
	}

	_, err := tx.Stmt(l.addField).Exec(args...)

	return err
}

// ListByOptions returns objects according to the specified list options and partitions
// result is an unstructured.UnstructuredList, the continue token for the next page (or an error)
func (l *ListOptionIndexer) ListByOptions(lo *listprocessor.ListOptions, partitions []listprocessor.Partition, namespace string) (*unstructured.UnstructuredList, string, error) {
	// 1- Intro: SELECT and JOIN clauses
	stmt := fmt.Sprintf(`SELECT o.object FROM "%s" o`, l.name)
	stmt += "\n  "
	stmt += fmt.Sprintf(`JOIN "%s_fields" f ON o.key = f.key`, l.name)
	params := []any{}

	// 2- Filtering: WHERE clauses (from lo.Filters)
	whereClauses := []string{}
	for _, orFilters := range lo.Filters {
		orClause, orParams := buildORClause(orFilters)
		whereClauses = append(whereClauses, orClause)
		params = append(params, orParams...)
	}

	// WHERE clauses (from namespace)
	if namespace != "" && namespace != "*" {
		whereClauses = append(whereClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
		params = append(params, namespace)
	}

	// WHERE clauses (from partitions and their corresponding parameters)
	partitionClauses := []string{}
	for _, partition := range partitions {
		if partition.Passthrough {
			// nothing to do, no extra filtering to apply by definition
		} else {
			singlePartitionClauses := []string{}

			// filter by namespace
			if partition.Namespace != "" && partition.Namespace != "*" {
				singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
				params = append(params, partition.Namespace)
			}

			// optionally filter by names
			if !partition.All {
				names := partition.Names

				if len(names) == 0 {
					// degenerate case, there will be no results
					singlePartitionClauses = append(singlePartitionClauses, "FALSE")
				} else {
					singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.name" IN (?%s)`, strings.Repeat(", ?", len(partition.Names)-1)))
					for name := range partition.Names {
						params = append(params, name)
					}
				}
			}

			if len(singlePartitionClauses) > 0 {
				partitionClauses = append(partitionClauses, strings.Join(singlePartitionClauses, " AND "))
			}
		}
	}
	if len(partitions) == 0 {
		// degenerate case, there will be no results
		whereClauses = append(whereClauses, "FALSE")
	}
	if len(partitionClauses) == 1 {
		whereClauses = append(whereClauses, partitionClauses[0])
	}
	if len(partitionClauses) > 1 {
		whereClauses = append(whereClauses, "(\n      ("+strings.Join(partitionClauses, ") OR\n      (")+")\n)")
	}

	if len(whereClauses) > 0 {
		stmt += "\n  WHERE\n    "
		stmt += strings.Join(whereClauses, " AND\n    ")
	}

	// 2- Sorting: ORDER BY clauses (from lo.Sort)
	orderByClauses := []string{}
	if len(lo.Sort.PrimaryField) > 0 {
		columnName := toColumnName(lo.Sort.PrimaryField)
		direction := "ASC"
		if lo.Sort.PrimaryOrder == listprocessor.DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`f."%s" %s`, columnName, direction))
	}
	if len(lo.Sort.SecondaryField) > 0 {
		columnName := toColumnName(lo.Sort.SecondaryField)
		direction := "ASC"
		if lo.Sort.SecondaryOrder == listprocessor.DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`f."%s" %s`, columnName, direction))
	}

	if len(orderByClauses) > 0 {
		stmt += "\n  ORDER BY "
		stmt += strings.Join(orderByClauses, ", ")
	} else {
		// make sure one default order is always picked
		stmt += "\n  ORDER BY f.\"metadata.name\" ASC "
	}

	// 3- Pagination: LIMIT clause (from lo.Pagination and/or lo.ChunkSize/lo.Resume)
	limitClause := ""
	offsetClause := ""
	// take the smallest limit between lo.Pagination and lo.ChunkSize
	limit := lo.Pagination.PageSize
	if limit == 0 || (lo.ChunkSize > 0 && lo.ChunkSize < limit) {
		limit = lo.ChunkSize
	}
	if limit > 0 {
		limitClause = "\n  LIMIT ?"
		// note: retrieve one extra row. If it comes back, then there are more pages and a continueToken should be created
		params = append(params, limit+1)
	}

	// OFFSET clause (from lo.Pagination and/or lo.Resume)
	offset := 0
	if lo.Resume != "" {
		offsetString := strings.Split(lo.Resume, ",")[1]
		offsetInt, err := strconv.Atoi(offsetString)
		if err != nil {
			return nil, "", err
		}
		offset = offsetInt
	}
	if lo.Pagination.Page >= 1 {
		offset += lo.Pagination.PageSize * (lo.Pagination.Page - 1)
	}

	if offset > 0 {
		offsetClause = "\n  OFFSET ?"
		params = append(params, offset)
	}

	stmt += limitClause
	stmt += offsetClause

	// log the final query
	logrus.Debugf("ListOptionIndexer prepared statement: %v", stmt)
	logrus.Debugf("Params: %v", params...)

	// execute
	items, err := l.QueryObjects(l.Prepare(stmt), params...)
	if err != nil {
		return nil, "", err
	}

	continueToken := ""
	if limit > 0 && len(items) == limit+1 {
		// remove extra row
		items = items[:limit]
		continueToken = fmt.Sprintf("%d", offset+limit)
	}

	return toUnstructuredList(items), continueToken, nil
}

/* Utilities */

// buildORClause creates an SQLite compatible query that ORs conditions built from passed filters
func buildORClause(orFilters listprocessor.OrFilter) (string, []any) {
	var orWhereClause string
	var params []any
	for index, filter := range orFilters.Filters {
		opString := "LIKE"
		if filter.Op == "!=" {
			opString = "NOT LIKE"
		}
		columnName := toColumnName(filter.Field)
		orWhereClause += fmt.Sprintf(`f."%s" %s ?`, columnName, opString)
		params = append(params, fmt.Sprintf(`%%%s%%`, filter.Match))
		if index == len(orFilters.Filters)-1 {
			continue
		}
		orWhereClause += " OR "
	}
	return orWhereClause, params
}

// toColumnName returns the column name corresponding to a field expressed as string slice
func toColumnName(s []string) string {
	return Sanitize(strings.Join(s, "."))
}

// getField extracts the value of a field expressed as a string path from an unstructured object
func getField(a any, field string) (any, error) {
	o, ok := a.(*unstructured.Unstructured)
	if !ok {
		return nil, errors.Errorf("Unexpected object type, expected unstructured.Unstructured: %v", a)
	}
	result, ok, err := unstructured.NestedFieldNoCopy(o.Object, strings.Split(field, ".")...)
	if !ok || err != nil {
		return nil, errors.Wrapf(err, "Could not extract field %v from object %v", field, o)
	}
	return result, nil
}

// toUnstructuredList turns a slice of unstructured objects into an unstructured.UnstructuredList
func toUnstructuredList(items []any) *unstructured.UnstructuredList {
	objectItems := make([]map[string]any, len(items))
	result := &unstructured.UnstructuredList{
		Items:  make([]unstructured.Unstructured, len(items)),
		Object: map[string]interface{}{"items": objectItems},
	}
	for i, item := range items {
		result.Items[i] = *item.(*unstructured.Unstructured)
		objectItems[i] = item.(*unstructured.Unstructured).Object
	}
	return result
}
