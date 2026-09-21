use quent_analyzer::{
    AnalyzerError, AnalyzerResult, Entity, Model, RefTreeEntity,
    ref_tree::RefTreeCollection,
    resource::{Resource, ResourceTypeDecl, collection::ResourceCollection},
};
use quent_query_engine_analyzer::{QueryEngineModel, plan_tree::PlanTree};
use quent_query_engine_ui::EntityRef;
use rustc_hash::FxHashMap as HashMap;
use uuid::Uuid;

use crate::{
    analyzed_entities::{Engine, Operator, Plan, Port, Query, QueryGroup, Worker},
    model::{DuckDbModel, RuntimeResource},
};

pub(crate) struct DuckDbQueryView<'a> {
    engine: &'a Engine,
    query_group: &'a QueryGroup,
    query: &'a Query,
    workers: HashMap<Uuid, &'a Worker>,
    plans: HashMap<Uuid, &'a Plan>,
    operators: HashMap<Uuid, &'a Operator>,
    ports: HashMap<Uuid, &'a Port>,
    pub(crate) resources: HashMap<Uuid, &'a RuntimeResource>,
    pub(crate) resource_types: HashMap<String, &'a ResourceTypeDecl>,
}

impl<'a> DuckDbQueryView<'a> {
    pub(crate) fn try_new(model: &'a DuckDbModel, query_id: Uuid) -> AnalyzerResult<Self> {
        let query = model.query(query_id)?;
        let query_group = model.query_group(
            quent_query_engine_analyzer::QueryEntity::query_group_id(query).unwrap_or_default(),
        )?;
        let workers: HashMap<_, _> = model
            .query_workers(query_id)?
            .map(|entity| (entity.id(), entity))
            .collect();
        let plans: HashMap<_, _> = model
            .query_plans(query_id)?
            .map(|entity| (entity.id(), entity))
            .collect();
        let operators: HashMap<_, _> = model
            .plans_operators(plans.values().copied())?
            .map(|entity| (entity.id(), entity))
            .collect();
        let ports = model
            .operators_ports(operators.values().copied())?
            .map(|entity| (entity.id(), entity))
            .collect();
        let resources = model
            .runtime_resources
            .iter()
            .filter(|(_, resource)| {
                resource.parent_id().is_some_and(|parent| {
                    parent == model.engine.id() || workers.contains_key(&parent)
                })
            })
            .map(|(id, resource)| (*id, resource))
            .collect();
        let resource_types = model
            .resource_types
            .iter()
            .map(|(name, declaration)| (name.clone(), declaration))
            .collect();
        Ok(Self {
            engine: &model.engine,
            query_group,
            query,
            workers,
            plans,
            operators,
            ports,
            resources,
            resource_types,
        })
    }
}

impl ResourceCollection for DuckDbQueryView<'_> {
    fn resources(&self) -> impl Iterator<Item = &dyn Resource> {
        self.resources
            .values()
            .map(|resource| *resource as &dyn Resource)
    }
    fn resource(&self, id: Uuid) -> AnalyzerResult<&dyn Resource> {
        self.resources
            .get(&id)
            .map(|resource| *resource as &dyn Resource)
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn resource_type(&self, name: &str) -> AnalyzerResult<&ResourceTypeDecl> {
        self.resource_types
            .get(name)
            .copied()
            .ok_or_else(|| AnalyzerError::InvalidTypeName(name.to_owned()))
    }
}

impl RefTreeCollection for DuckDbQueryView<'_> {
    fn ref_tree_entities(&self) -> impl Iterator<Item = &dyn RefTreeEntity> {
        std::iter::once(self.engine as &dyn RefTreeEntity)
            .chain(std::iter::once(self.query_group as &dyn RefTreeEntity))
            .chain(std::iter::once(self.query as &dyn RefTreeEntity))
            .chain(
                self.workers
                    .values()
                    .map(|entity| *entity as &dyn RefTreeEntity),
            )
            .chain(
                self.plans
                    .values()
                    .map(|entity| *entity as &dyn RefTreeEntity),
            )
            .chain(
                self.operators
                    .values()
                    .map(|entity| *entity as &dyn RefTreeEntity),
            )
            .chain(
                self.ports
                    .values()
                    .map(|entity| *entity as &dyn RefTreeEntity),
            )
            .chain(
                self.resources
                    .values()
                    .map(|entity| *entity as &dyn RefTreeEntity),
            )
    }

    fn ref_tree_entity(&self, id: Uuid) -> AnalyzerResult<&dyn RefTreeEntity> {
        if self.engine.id() == id {
            return Ok(self.engine);
        }
        if self.query_group.id() == id {
            return Ok(self.query_group);
        }
        if self.query.id() == id {
            return Ok(self.query);
        }
        self.workers
            .get(&id)
            .map(|entity| *entity as &dyn RefTreeEntity)
            .or_else(|| {
                self.plans
                    .get(&id)
                    .map(|entity| *entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.operators
                    .get(&id)
                    .map(|entity| *entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.ports
                    .get(&id)
                    .map(|entity| *entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.resources
                    .get(&id)
                    .map(|entity| *entity as &dyn RefTreeEntity)
            })
            .ok_or(AnalyzerError::InvalidId(id))
    }
}

impl QueryEngineModel for DuckDbQueryView<'_> {
    type Engine = Engine;
    type Query = Query;
    type QueryGroup = QueryGroup;
    type Worker = Worker;
    type Plan = Plan;
    type Operator = Operator;
    type Port = Port;

    fn engine(&self) -> AnalyzerResult<&Engine> {
        Ok(self.engine)
    }
    fn query(&self, id: Uuid) -> AnalyzerResult<&Query> {
        (self.query.id() == id)
            .then_some(self.query)
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn query_group(&self, id: Uuid) -> AnalyzerResult<&QueryGroup> {
        (self.query_group.id() == id)
            .then_some(self.query_group)
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn worker(&self, id: Uuid) -> AnalyzerResult<&Worker> {
        self.workers
            .get(&id)
            .copied()
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn plan(&self, id: Uuid) -> AnalyzerResult<&Plan> {
        self.plans
            .get(&id)
            .copied()
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn operator(&self, id: Uuid) -> AnalyzerResult<&Operator> {
        self.operators
            .get(&id)
            .copied()
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn port(&self, id: Uuid) -> AnalyzerResult<&Port> {
        self.ports
            .get(&id)
            .copied()
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn queries(&self) -> impl Iterator<Item = &Query> {
        std::iter::once(self.query)
    }
    fn query_groups(&self) -> impl Iterator<Item = &QueryGroup> {
        std::iter::once(self.query_group)
    }
    fn workers(&self) -> impl Iterator<Item = &Worker> {
        self.workers.values().copied()
    }
    fn plans(&self) -> impl Iterator<Item = &Plan> {
        self.plans.values().copied()
    }
    fn operators(&self) -> impl Iterator<Item = &Operator> {
        self.operators.values().copied()
    }
    fn ports(&self) -> impl Iterator<Item = &Port> {
        self.ports.values().copied()
    }
    fn plan_tree(&self, query_id: Uuid) -> AnalyzerResult<PlanTree> {
        PlanTree::try_new(self.plans.values().copied(), query_id)
    }
}

impl Model for DuckDbQueryView<'_> {
    type EntityIdType = EntityRef;
    fn try_entity_ref(&self, id: Uuid) -> AnalyzerResult<EntityRef> {
        if self.engine.id() == id {
            Ok(EntityRef::Engine(id))
        } else if self.workers.contains_key(&id) {
            Ok(EntityRef::Worker(id))
        } else if self.query_group.id() == id {
            Ok(EntityRef::QueryGroup(id))
        } else if self.query.id() == id {
            Ok(EntityRef::Query(id))
        } else if self.plans.contains_key(&id) {
            Ok(EntityRef::Plan(id))
        } else if self.operators.contains_key(&id) {
            Ok(EntityRef::Operator(id))
        } else if self.ports.contains_key(&id) {
            Ok(EntityRef::Port(id))
        } else if self.resources.contains_key(&id) {
            Ok(EntityRef::Resource(id))
        } else {
            Err(AnalyzerError::InvalidId(id))
        }
    }
}
