mod insert;
mod insert_node;
mod insert_node_alias;
mod insert_node_values;
mod insert_nodes;
mod insert_nodes_aliases;
mod insert_nodes_count;
mod insert_nodes_values;

use self::insert::InsertBuilder;

pub struct QueryBuilder {}

impl QueryBuilder {
    pub fn insert() -> InsertBuilder {
        InsertBuilder {}
    }
}
