use crate::DbError;
use crate::DbImpl;
use crate::QueryCondition;
use crate::StorageData;
use crate::graph::GraphIndex;
use crate::graph_search::SearchControl;
use crate::graph_search::SearchHandler;
use crate::graph_search::path_search::PathSearchHandler;

pub struct DefaultHandler<'a, Store: StorageData> {
    db: &'a DbImpl<Store>,
    conditions: &'a Vec<QueryCondition>,
}

pub struct LimitHandler<'a, Store: StorageData> {
    limit: u64,
    counter: u64,
    db: &'a DbImpl<Store>,
    conditions: &'a Vec<QueryCondition>,
}

pub struct OffsetHandler<'a, Store: StorageData> {
    offset: u64,
    counter: u64,
    db: &'a DbImpl<Store>,
    conditions: &'a Vec<QueryCondition>,
}

pub struct LimitOffsetHandler<'a, Store: StorageData> {
    limit: u64,
    offset: u64,
    counter: u64,
    db: &'a DbImpl<Store>,
    conditions: &'a Vec<QueryCondition>,
}

pub struct PathHandler<'a, Store: StorageData> {
    db: &'a DbImpl<Store>,
    conditions: &'a Vec<QueryCondition>,
}

impl<'a, Store: StorageData> DefaultHandler<'a, Store> {
    pub fn new(db: &'a DbImpl<Store>, conditions: &'a Vec<QueryCondition>) -> Self {
        Self { db, conditions }
    }
}

impl<'a, Store: StorageData> LimitHandler<'a, Store> {
    pub fn new(limit: u64, db: &'a DbImpl<Store>, conditions: &'a Vec<QueryCondition>) -> Self {
        Self {
            limit,
            counter: 0,
            db,
            conditions,
        }
    }
}

impl<'a, Store: StorageData> OffsetHandler<'a, Store> {
    pub fn new(offset: u64, db: &'a DbImpl<Store>, conditions: &'a Vec<QueryCondition>) -> Self {
        Self {
            offset,
            counter: 0,
            db,
            conditions,
        }
    }
}

impl<'a, Store: StorageData> LimitOffsetHandler<'a, Store> {
    pub fn new(
        limit: u64,
        offset: u64,
        db: &'a DbImpl<Store>,
        conditions: &'a Vec<QueryCondition>,
    ) -> Self {
        Self {
            limit: limit + offset,
            offset,
            counter: 0,
            db,
            conditions,
        }
    }
}

impl<'a, Store: StorageData> PathHandler<'a, Store> {
    pub fn new(db: &'a DbImpl<Store>, conditions: &'a Vec<QueryCondition>) -> Self {
        Self { db, conditions }
    }
}

impl<Store: StorageData> SearchHandler for DefaultHandler<'_, Store> {
    fn process(&mut self, index: GraphIndex, distance: u64) -> Result<SearchControl, DbError> {
        self.db
            .evaluate_conditions(index, distance, self.conditions)
    }
}

impl<Store: StorageData> SearchHandler for LimitHandler<'_, Store> {
    fn process(&mut self, index: GraphIndex, distance: u64) -> Result<SearchControl, DbError> {
        let control = self
            .db
            .evaluate_conditions(index, distance, self.conditions)?;
        let add = control.is_true();

        if add {
            self.counter += 1;
        }

        if self.counter == self.limit {
            Ok(SearchControl::Finish(add))
        } else {
            Ok(control)
        }
    }
}

impl<Store: StorageData> SearchHandler for OffsetHandler<'_, Store> {
    fn process(&mut self, index: GraphIndex, distance: u64) -> Result<SearchControl, DbError> {
        let mut control = self
            .db
            .evaluate_conditions(index, distance, self.conditions)?;

        if control.is_true() {
            self.counter += 1;
            control.set_value(self.offset < self.counter);
        }

        Ok(control)
    }
}

impl<Store: StorageData> SearchHandler for LimitOffsetHandler<'_, Store> {
    fn process(&mut self, index: GraphIndex, distance: u64) -> Result<SearchControl, DbError> {
        let mut control = self
            .db
            .evaluate_conditions(index, distance, self.conditions)?;

        if control.is_true() {
            self.counter += 1;
            control.set_value(self.offset < self.counter);
        }

        if self.counter == self.limit {
            Ok(SearchControl::Finish(control.is_true()))
        } else {
            Ok(control)
        }
    }
}

impl<Store: StorageData> PathSearchHandler for PathHandler<'_, Store> {
    fn process(&self, index: GraphIndex, distance: u64) -> Result<(u64, bool), DbError> {
        match self
            .db
            .evaluate_conditions(index, distance, self.conditions)?
        {
            SearchControl::Continue(add) => Ok((1 + ((!add) as u64), add)),
            SearchControl::Finish(add) | SearchControl::Stop(add) => Ok((0, add)),
        }
    }
}
