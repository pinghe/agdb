use crate::search_index::SearchIndex;
use crate::search_iterator::SearchIterator;

pub(crate) struct DepthFirstSearch {
    index: Option<SearchIndex>,
}

impl SearchIterator for DepthFirstSearch {
    fn expand_edge<Data: agdb_graph::GraphData>(
        index: &agdb_graph::GraphIndex,
        graph: &agdb_graph::GraphImpl<Data>,
    ) -> agdb_graph::GraphIndex {
        graph
            .edge(index)
            .expect("invalid index, expected a valid edge index")
            .index_to()
    }

    fn expand_node<Data: agdb_graph::GraphData>(
        index: &agdb_graph::GraphIndex,
        graph: &agdb_graph::GraphImpl<Data>,
    ) -> Vec<agdb_graph::GraphIndex> {
        graph
            .node(index)
            .expect("invalid index, expected a valid node index")
            .edge_iter_from()
            .map(|edge| edge.index())
            .collect()
    }

    fn new(stack: &mut Vec<SearchIndex>) -> Self {
        Self { index: stack.pop() }
    }

    fn next(&mut self) -> Option<SearchIndex> {
        self.index.take()
    }
}
