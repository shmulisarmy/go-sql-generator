package main

import (
	"fmt"
	"reflect"
	"strings"
)

type Query struct {
	args []interface{}
}

func SubQuery(sub fmt.Stringer) string {
	return fmt.Sprintf("(%s)", sub.String())
}

func (q *Query) Args() []interface{} {
	return q.args
}

type Select struct {
	Query
	select_section string
	from_section   string
	join_section   string
	where_section  string
	group_section  string
	having_section string
	order_section  string
	limit_section  string
	offset_section string
}

type Insert struct {
	Query
	table   string
	columns []string
	values  [][]interface{}
}

type Update struct {
	Query
	table       string
	set_section string
	where       string
}

type Delete struct {
	Query
	table string
	where string
}

func (s *Select) Select(columns ...string) *Select {
	if s.select_section != "" {
		s.select_section += ", "
	}
	s.select_section += strings.Join(columns, ", ")
	return s
}

func (s *Select) From(table string) *Select {
	s.from_section = table
	return s
}

func (s *Select) Join(table, onCondition string) *Select {
	s.join_section += fmt.Sprintf(" JOIN %s ON %s", table, onCondition)
	return s
}

func (s *Select) LeftJoin(table, onCondition string) *Select {
	s.join_section += fmt.Sprintf(" LEFT JOIN %s ON %s", table, onCondition)
	return s
}

func (s *Select) Where(condition string, args ...interface{}) *Select {
	if s.where_section != "" {
		s.where_section += " AND "
	}
	s.where_section += condition
	s.args = append(s.args, args...)
	return s
}

func (s *Select) GroupBy(columns ...string) *Select {
	s.group_section = strings.Join(columns, ", ")
	return s
}

func (s *Select) Having(condition string, args ...interface{}) *Select {
	s.having_section = condition
	s.args = append(s.args, args...)
	return s
}

func (s *Select) OrderBy(order string) *Select {
	s.order_section = order
	return s
}

func (s *Select) Limit(n int) *Select {
	s.limit_section = fmt.Sprintf("%d", n)
	return s
}

func (s *Select) Offset(n int) *Select {
	s.offset_section = fmt.Sprintf("%d", n)
	return s
}

func (s *Select) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT " + s.select_section)
	if s.from_section != "" {
		sb.WriteString(" FROM " + s.from_section)
	}
	sb.WriteString(s.join_section)
	if s.where_section != "" {
		sb.WriteString(" WHERE " + s.where_section)
	}
	if s.group_section != "" {
		sb.WriteString(" GROUP BY " + s.group_section)
	}
	if s.having_section != "" {
		sb.WriteString(" HAVING " + s.having_section)
	}
	if s.order_section != "" {
		sb.WriteString(" ORDER BY " + s.order_section)
	}
	if s.limit_section != "" {
		sb.WriteString(" LIMIT " + s.limit_section)
	}
	if s.offset_section != "" {
		sb.WriteString(" OFFSET " + s.offset_section)
	}
	return sb.String()
}

func (i *Insert) Into(table string, columns ...string) *Insert {
	i.table = table
	i.columns = columns
	return i
}

func (i *Insert) Values(vals ...interface{}) *Insert {
	i.values = append(i.values, vals)
	i.args = append(i.args, vals...)
	return i
}

func (i *Insert) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", i.table, strings.Join(i.columns, ", ")))
	placeholders := make([]string, len(i.columns))
	for j := range placeholders {
		placeholders[j] = "?"
	}
	valuesStr := make([]string, len(i.values))
	for j := range i.values {
		valuesStr[j] = "(" + strings.Join(placeholders, ", ") + ")"
	}
	sb.WriteString(strings.Join(valuesStr, ", "))
	return sb.String()
}

func (u *Update) Table(table string) *Update {
	u.table = table
	return u
}

func (u *Update) Set(assignments string, args ...interface{}) *Update {
	u.set_section = assignments
	u.args = append(u.args, args...)
	return u
}

func (u *Update) Where(condition string, args ...interface{}) *Update {
	if u.where != "" {
		u.where += " AND "
	}
	u.where += condition
	u.args = append(u.args, args...)
	return u
}

func (u *Update) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("UPDATE %s SET %s", u.table, u.set_section))
	if u.where != "" {
		sb.WriteString(" WHERE " + u.where)
	}
	return sb.String()
}

func (d *Delete) From(table string) *Delete {
	d.table = table
	return d
}

func (d *Delete) Where(condition string, args ...interface{}) *Delete {
	if d.where != "" {
		d.where += " AND "
	}
	d.where += condition
	d.args = append(d.args, args...)
	return d
}

func (d *Delete) String() string {
	var sb strings.Builder
	sb.WriteString("DELETE FROM " + d.table)
	if d.where != "" {
		sb.WriteString(" WHERE " + d.where)
	}
	return sb.String()
}

func Display(label string, q fmt.Stringer, args []interface{}) {
	fmt.Println(label + ":")
	fmt.Println("SQL: ", q.String())
	fmt.Println("ARGS:", args)
	fmt.Println()
}

type User struct {
	Name          string `json:"name"`
	Email         string `json:"email"`
	Age           int    `json:"age"`
	OthersRefered []User `json:"othersRefered" one-to-many:"refered_by"`
	WorksFor      []User `json:"worksFor" many-to-many:"boss-workers"`
	Workers       []User `json:"workers" many-to-many:"worker-worksFor"`
}

func (insert *Insert) from_object(obj interface{}) *Insert {
	if len(insert.columns) > 0 {
		panic("you cant all from from_object after there are already columns on the insert object")
	}
	if len(insert.values) > 0 {
		panic("this must be the first set of values that you put on the insert object")
	}
	types := reflect.TypeOf(obj)
	values := reflect.ValueOf(obj)

	first_set_of_values := make([]interface{}, 0, types.NumField())

	for i := 0; i < types.NumField(); i++ {
		jsonTag := types.Field(i).Tag.Get("json")
		if jsonTag != "" {
			fmt.Printf("adding column %s\n with value %v\n", jsonTag, values.Field(i).Interface())
			insert.columns = append(insert.columns, jsonTag)
			first_set_of_values = append(first_set_of_values, values.Field(i).Interface())
		} else {
			panic("cannot do proper reflection on object because there is no json tag")
		}
	}
	insert.Values(first_set_of_values...)
	return insert
}

var fake_ids = 12

func (insert *Insert) fake_insert_execution() int {
	fmt.Printf("(fake) executing %s with args %v\n", insert.String(), insert.Args())
	fake_id := fake_ids
	fake_ids++
	return fake_id
}

func (insert *Insert) from_many_objects(objects []interface{}) *Insert {
	if len(insert.columns) > 0 {
		panic("you can't call from_many_objects after there are already columns on the insert object")
	}
	if len(insert.values) > 0 {
		panic("this must be the first set of values you put on the insert object")
	}
	if len(objects) == 0 {
		panic("you must provide at least one object to insert")
	}
	insert.from_object(objects[0])
	for _, obj := range objects[1:] {
		insert.Values(obj)
	}

	return insert
}

// recursive_create inserts the given object and any nested one-to-many children.
// extras allows injecting additional columns (e.g., refered_by) for the insert.
func recursive_create(obj interface{}, extras map[string]interface{}) int {
	typ := reflect.TypeOf(obj)
	val := reflect.ValueOf(obj)

	// Prepare Insert
	i := &Insert{}
	columns := []string{}
	values := []interface{}{}

	// Collect fields except one-to-many
	for j := 0; j < typ.NumField(); j++ {
		field := typ.Field(j)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			panic("cannot do proper reflection on object because there is no json tag")
		}
		if field.Tag.Get("relation") == "one-to-many" || field.Tag.Get("relation") == "many-to-many" {
			// skip nested slices here
			continue
		}
		columns = append(columns, jsonTag)
		values = append(values, val.Field(j).Interface())
	}

	// Apply extras if any
	for col, v := range extras {
		columns = append(columns, col)
		values = append(values, v)
	}

	// Build insert
	i.table = reflect.TypeOf(obj).Name() // or customize table naming
	i.columns = columns
	i.values = append(i.values, values)
	i.args = append(i.args, values...)

	// Fake execution
	id := i.fake_insert_execution()

	// Recurse into one-to-many fields
	for j := 0; j < typ.NumField(); j++ {
		field := typ.Field(j)
		if field.Tag.Get("one-to-many") != "" {
			// Extract slice
			slice := val.Field(j)
			// Each element must be addressable or a struct
			for k := 0; k < slice.Len(); k++ {
				child := slice.Index(k).Interface()
				// Pass refered_by to child
				extra := map[string]interface{}{field.Tag.Get("one-to-many"): id}
				recursive_create(child, extra)
			}
		} else if field.Tag.Get("many-to-many") != "" {
			// Extract slice
			type_in_list := field.Type.Elem()
			this_structFields_relationship_status := field.Tag.Get("many-to-many")
			related_structFields_relationship_status := find_by_json_tag(type_in_list, strings.Split(this_structFields_relationship_status, "-")[1]).Tag.Get(("many-to-many"))
			join_table_name := strings.Split(this_structFields_relationship_status, "-")[0] + "_" + strings.Split(related_structFields_relationship_status, "-")[0]
			fmt.Println("json tag:", field.Tag.Get("json"))
			fmt.Println("type in list:", type_in_list.String())
			fmt.Println("many-to-many relationship status:", related_structFields_relationship_status)
			fmt.Println("join table name:", join_table_name)
		}
	}

	return id
}

func find_by_json_tag(type_ reflect.Type, json_tag string) reflect.StructField {
	fmt.Printf("json tag: %s %s\n", json_tag, type_)
	for i := 0; i < type_.NumField(); i++ {
		field := type_.Field(i)
		if field.Tag.Get("json") == json_tag {
			return field
		}
	}
	panic("field not found")

}

func main() {
	u := User{
		Name:  "shmuli",
		Email: "shmuli@example.com",
		Age:   30,
		OthersRefered: []User{
			{
				Name:  "berel",
				Email: "berel@example.com",
				Age:   30,
			},
			{
				Name:  "lev",
				Email: "lev@example.com",
				Age:   30,
				OthersRefered: []User{
					{
						Name:  "fag",
						Email: "fag.com",
						Age:   21,
					},
				},
			},
		},
	}
	recursive_create(u, nil)
}
