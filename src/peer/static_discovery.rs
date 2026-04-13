use anyhow::Result;

pub struct StaticDiscovery {
    hosts: Vec<String>,
}

impl StaticDiscovery {
    pub fn new(hosts: Vec<String>) -> Self {
        Self { hosts }
    }

    pub async fn discover(&self) -> Result<Vec<String>> {
        Ok(self.hosts.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_configured_hosts() {
        let hosts = vec!["a:1".to_string(), "b:2".to_string()];
        let disc = StaticDiscovery::new(hosts.clone());
        assert_eq!(disc.discover().await.unwrap(), hosts);
    }

    #[tokio::test]
    async fn empty_hosts() {
        let disc = StaticDiscovery::new(vec![]);
        assert!(disc.discover().await.unwrap().is_empty());
    }
}
