use std::collections::HashMap;
use tokio::sync::RwLock;

use sea_common::{NodeInfo, PortDecl, SeaError, SeaResult};

/// 节点目录服务 —— 存储所有节点的外部三要素。
///
/// 仅保存 id、description、ports，不保存 runtime 细节。
/// 支持查询、注册、注销。
pub struct Registry {
    catalog: RwLock<HashMap<String, NodeInfo>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            catalog: RwLock::new(HashMap::new()),
        }
    }

    /// 注册节点。
    pub async fn register(&self, info: NodeInfo) -> SeaResult<()> {
        let mut catalog = self.catalog.write().await;
        catalog.insert(info.id.clone(), info);
        Ok(())
    }

    /// 注销节点。
    pub async fn unregister(&self, node_id: &str) -> SeaResult<()> {
        let mut catalog = self.catalog.write().await;
        catalog.remove(node_id);
        Ok(())
    }

    /// 按 id 精确查询。
    pub async fn query_by_id(&self, node_id: &str) -> SeaResult<NodeInfo> {
        let catalog = self.catalog.read().await;
        catalog
            .get(node_id)
            .cloned()
            .ok_or_else(|| SeaError::NodeNotFound(node_id.to_string()))
    }

    /// 按能力描述模糊匹配。
    pub async fn query_by_capability(&self, capability: &str) -> SeaResult<Vec<NodeInfo>> {
        let catalog = self.catalog.read().await;
        let mut results: Vec<NodeInfo> = catalog
            .values()
            .filter(|info| {
                info.description
                    .to_lowercase()
                    .contains(&capability.to_lowercase())
            })
            .cloned()
            .collect();

        // 按匹配度排序：关键字出现在描述开头 > 描述越短匹配度越高
        results.sort_by(|a, b| {
            let a_pos = a
                .description
                .to_lowercase()
                .find(&capability.to_lowercase())
                .unwrap_or(usize::MAX);
            let b_pos = b
                .description
                .to_lowercase()
                .find(&capability.to_lowercase())
                .unwrap_or(usize::MAX);
            a_pos.cmp(&b_pos).then(a.description.len().cmp(&b.description.len()))
        });

        Ok(results)
    }

    /// 列出全部节点。
    pub async fn list_all(&self) -> Vec<NodeInfo> {
        let catalog = self.catalog.read().await;
        catalog.values().cloned().collect()
    }

    /// 生成供 LLM 注入的文本摘要。
    pub async fn generate_summary(&self) -> String {
        let catalog = self.catalog.read().await;
        if catalog.is_empty() {
            return "## 可用节点\n\n暂无可用节点".to_string();
        }

        let mut lines: Vec<String> = Vec::new();
        let mut sorted_ids: Vec<&String> = catalog.keys().collect();
        sorted_ids.sort();

        for id in sorted_ids {
            if let Some(info) = catalog.get(id) {
                let inputs_desc = info
                    .inputs
                    .iter()
                    .map(|p| format!("{}({})", p.port, p.format))
                    .collect::<Vec<_>>()
                    .join(", ");
                let outputs_desc = info
                    .outputs
                    .iter()
                    .map(|p| format!("{}({})", p.port, p.format))
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.push(format!(
                    "- `{}`: {} [in: {}] [out: {}]",
                    info.id, info.description, inputs_desc, outputs_desc
                ));
            }
        }

        format!("## 可用节点\n\n{}", lines.join("\n"))
    }

    /// 更新节点描述。
    pub async fn update_description(&self, node_id: &str, description: &str) -> SeaResult<()> {
        let mut catalog = self.catalog.write().await;
        if let Some(info) = catalog.get_mut(node_id) {
            info.description = description.to_string();
            Ok(())
        } else {
            Err(SeaError::NodeNotFound(node_id.to_string()))
        }
    }

    /// 更新节点端口。
    pub async fn update_ports(
        &self,
        node_id: &str,
        inputs: Vec<PortDecl>,
        outputs: Vec<PortDecl>,
    ) -> SeaResult<()> {
        let mut catalog = self.catalog.write().await;
        if let Some(info) = catalog.get_mut(node_id) {
            info.inputs = inputs;
            info.outputs = outputs;
            Ok(())
        } else {
            Err(SeaError::NodeNotFound(node_id.to_string()))
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}
