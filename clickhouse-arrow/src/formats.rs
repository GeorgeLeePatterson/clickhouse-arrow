mod arrow;
mod native;
pub(crate) mod protocol_data;

// Re-exports
pub use arrow::ArrowFormat;
pub use native::NativeFormat;

use crate::{ArrowOptions, Type};

/// Marker trait for various client formats.
///
/// Currently only two formats are in use: `ArrowFormat` and `NativeFormat`. This approach provides
/// a simple mechanism to introduce new formats to work with `ClickHouse` data without a lot of
/// overhead and a fullblown serde implementation.
#[expect(private_bounds)]
pub trait ClientFormat: sealed::ClientFormatImpl<Self::Data> + Send + Sync + 'static {
    type Data: std::fmt::Debug + Clone + Send + Sync + 'static;

    const FORMAT: &'static str;
}

pub(crate) mod sealed {
    use super::{DeserializerState, SerializerState};
    use crate::Type;
    use crate::client::connection::ClientMetadata;
    use crate::errors::Result;
    use crate::io::{ClickHouseRead, ClickHouseWrite};
    use crate::query::Qid;

    pub(crate) trait ClientFormatImpl<T>: std::fmt::Debug
    where
        T: std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        type Schema: std::fmt::Debug + Clone + Send + Sync + 'static;
        type Deser: Default + Send + Sync + 'static;
        type Ser: Default + Send + Sync + 'static;

        #[expect(unused)]
        fn finish_ser(_state: &mut SerializerState<Self::Ser>) {}

        fn finish_deser(_state: &mut DeserializerState<Self::Deser>) {}

        fn write<'a, W: ClickHouseWrite>(
            writer: &'a mut W,
            data: T,
            qid: Qid,
            header: Option<&'a [(String, Type)]>,
            revision: u64,
            metadata: ClientMetadata,
        ) -> impl Future<Output = Result<()>> + Send + 'a;

        fn read<'a, R: ClickHouseRead + 'static>(
            reader: &'a mut R,
            revision: u64,
            metadata: ClientMetadata,
            state: &'a mut DeserializerState<Self::Deser>,
        ) -> impl Future<Output = Result<Option<T>>> + Send + 'a;
    }
}

#[cfg(feature = "extended-types")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DynamicPrefixState {
    pub(crate) serialization_version: u64,
    pub(crate) flattened_types:       Vec<Type>,
}

#[cfg(feature = "extended-types")]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub(crate) struct VariantPrefixState {
    pub(crate) discriminator_mode: u8,
}

pub(crate) type CustomPlanNodeId = u32;
pub(crate) const CUSTOM_PLAN_NO_NODE: CustomPlanNodeId = u32::MAX;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CustomPlanNode {
    pub(crate) stack_type:     u8,
    pub(crate) kinds:          Vec<u8>,
    pub(crate) edge_start:     u32,
    pub(crate) edge_len:       u16,
    #[cfg(feature = "extended-types")]
    pub(crate) dynamic_prefix: Option<DynamicPrefixState>,
    #[cfg(feature = "extended-types")]
    pub(crate) variant_prefix: Option<VariantPrefixState>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CustomPlan {
    pub(crate) nodes: Vec<CustomPlanNode>,
    pub(crate) edges: Vec<CustomPlanNodeId>,
    pub(crate) root:  CustomPlanNodeId,
}

impl CustomPlanNode {
    #[must_use]
    pub(crate) fn is_sparse(&self) -> bool {
        self.stack_type == 1
            || self.stack_type == 3
            || (self.stack_type == 5 && self.kinds.contains(&1))
    }
}

impl CustomPlan {
    #[must_use]
    pub(crate) fn node(&self, node_id: CustomPlanNodeId) -> Option<&CustomPlanNode> {
        usize::try_from(node_id).ok().and_then(|idx| self.nodes.get(idx))
    }

    #[cfg(feature = "extended-types")]
    #[must_use]
    pub(crate) fn node_mut(&mut self, node_id: CustomPlanNodeId) -> Option<&mut CustomPlanNode> {
        usize::try_from(node_id).ok().and_then(|idx| self.nodes.get_mut(idx))
    }

    #[must_use]
    pub(crate) fn child(
        &self,
        node_id: CustomPlanNodeId,
        child_index: usize,
    ) -> Option<CustomPlanNodeId> {
        let node = self.node(node_id)?;
        let edge_start = usize::try_from(node.edge_start).ok()?;
        let edge_len = usize::from(node.edge_len);
        if child_index >= edge_len {
            return None;
        }
        self.edges.get(edge_start + child_index).copied()
    }

    #[must_use]
    pub(crate) fn from_type_structure(type_: &Type) -> Self {
        struct Frame<'a> {
            type_:      &'a Type,
            node_id:    CustomPlanNodeId,
            next_child: usize,
        }

        let mut nodes = vec![CustomPlanNode {
            stack_type: 0,
            kinds: Vec::new(),
            edge_start: 0,
            edge_len: 0,
            #[cfg(feature = "extended-types")]
            dynamic_prefix: None,
            #[cfg(feature = "extended-types")]
            variant_prefix: None,
        }];
        let mut edges = Vec::new();

        let mut stack = vec![Frame { type_, node_id: 0, next_child: 0 }];
        while let Some(frame) = stack.last_mut() {
            let Type::Tuple(inner) = frame.type_ else {
                let _ = stack.pop();
                continue;
            };
            if frame.next_child >= inner.len() {
                let _ = stack.pop();
                continue;
            }
            let child_type = &inner[frame.next_child].1;
            frame.next_child += 1;

            let node_id = u32::try_from(nodes.len()).unwrap_or(CUSTOM_PLAN_NO_NODE);
            let edge_start = u32::try_from(edges.len()).unwrap_or(u32::MAX);
            nodes.push(CustomPlanNode {
                stack_type: 0,
                kinds: Vec::new(),
                edge_start,
                edge_len: 0,
                #[cfg(feature = "extended-types")]
                dynamic_prefix: None,
                #[cfg(feature = "extended-types")]
                variant_prefix: None,
            });
            edges.push(node_id);
            let parent_idx = usize::try_from(frame.node_id).unwrap_or(usize::MAX);
            if let Some(parent) = nodes.get_mut(parent_idx) {
                parent.edge_len = parent.edge_len.saturating_add(1);
            }
            stack.push(Frame { type_: child_type, node_id, next_child: 0 });
        }

        Self { nodes, edges, root: 0 }
    }
}

/// Context maintained during deserialization
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DeserializerState<T: Default = ()> {
    pub(crate) format_state: T,
    custom_plan:             Option<CustomPlan>,
    custom_node:             Option<CustomPlanNodeId>,
}

impl<T: Default> DeserializerState<T> {
    #[must_use]
    pub(crate) fn format_state(&mut self) -> &mut T { &mut self.format_state }

    pub(crate) fn replace_custom_plan(&mut self, custom_plan: CustomPlan) -> Option<CustomPlan> {
        self.custom_node = Some(custom_plan.root);
        self.custom_plan.replace(custom_plan)
    }

    pub(crate) fn take_custom_plan(&mut self) -> Option<CustomPlan> {
        self.custom_node = None;
        self.custom_plan.take()
    }

    #[cfg(feature = "extended-types")]
    #[must_use]
    pub(crate) fn custom_plan(&self) -> Option<&CustomPlan> { self.custom_plan.as_ref() }

    #[must_use]
    pub(crate) fn custom_node(&self) -> Option<CustomPlanNodeId> { self.custom_node }

    pub(crate) fn set_custom_node(
        &mut self,
        custom_node: Option<CustomPlanNodeId>,
    ) -> Option<CustomPlanNodeId> {
        std::mem::replace(&mut self.custom_node, custom_node)
    }

    pub(crate) fn reset_custom_node_to_root(&mut self) {
        self.custom_node = self.custom_plan.as_ref().map(|plan| plan.root);
    }

    #[must_use]
    pub(crate) fn custom_child_node(&self, child_index: usize) -> Option<CustomPlanNodeId> {
        let node_id = self.custom_node?;
        self.custom_plan.as_ref()?.child(node_id, child_index)
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn replace_current_dynamic_prefix(
        &mut self,
        dynamic_prefix: DynamicPrefixState,
    ) -> Option<DynamicPrefixState> {
        let node_id = self.custom_node?;
        self.custom_plan
            .as_mut()
            .and_then(|plan| plan.node_mut(node_id))
            .and_then(|node| node.dynamic_prefix.replace(dynamic_prefix))
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn replace_current_variant_prefix(
        &mut self,
        variant_prefix: VariantPrefixState,
    ) -> Option<VariantPrefixState> {
        let node_id = self.custom_node?;
        self.custom_plan
            .as_mut()
            .and_then(|plan| plan.node_mut(node_id))
            .and_then(|node| node.variant_prefix.replace(variant_prefix))
    }
}

/// Context maintained during serialization
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SerializerState<T: Default = ()> {
    pub(crate) options:    Option<ArrowOptions>,
    pub(crate) serializer: T,
    custom_plan:           Option<CustomPlan>,
    #[cfg(feature = "extended-types")]
    dynamic_prefix:        Option<DynamicPrefixState>,
}

impl<T: Default> SerializerState<T> {
    #[must_use]
    pub(crate) fn with_arrow_options(mut self, options: ArrowOptions) -> Self {
        self.options = Some(options);
        self
    }

    #[expect(unused)]
    #[must_use]
    pub(crate) fn serializer(&mut self) -> &mut T { &mut self.serializer }

    #[expect(dead_code, reason = "Reserved for future custom serialization writes")]
    pub(crate) fn replace_custom_plan(&mut self, custom_plan: CustomPlan) -> Option<CustomPlan> {
        self.custom_plan.replace(custom_plan)
    }

    #[expect(dead_code, reason = "Reserved for future custom serialization writes")]
    pub(crate) fn take_custom_plan(&mut self) -> Option<CustomPlan> { self.custom_plan.take() }

    #[cfg(feature = "extended-types")]
    pub(crate) fn replace_dynamic_prefix(
        &mut self,
        dynamic_prefix: DynamicPrefixState,
    ) -> Option<DynamicPrefixState> {
        self.dynamic_prefix.replace(dynamic_prefix)
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn take_dynamic_prefix(&mut self) -> Option<DynamicPrefixState> {
        self.dynamic_prefix.take()
    }
}
