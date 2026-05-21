use crate::{config::DestinationConfig, resend::ResendEventForRouting};

#[derive(Debug, Clone)]
pub struct RouteMatcher {
    destinations: Vec<DestinationConfig>,
}

impl RouteMatcher {
    pub fn new(destinations: Vec<DestinationConfig>) -> Self {
        let destinations = destinations
            .into_iter()
            .map(normalize_destination)
            .collect::<Vec<_>>();

        Self { destinations }
    }

    pub fn match_destinations(&self, event: &ResendEventForRouting) -> Vec<DestinationConfig> {
        self.destinations
            .iter()
            .filter(|destination| destination_matches(destination, event))
            .cloned()
            .collect()
    }
}

fn destination_matches(destination: &DestinationConfig, event: &ResendEventForRouting) -> bool {
    if destination.catch_all {
        return true;
    }

    if !destination.event_types.is_empty() {
        let Some(event_type) = &event.event_type else {
            return false;
        };

        if !destination
            .event_types
            .iter()
            .any(|item| item == event_type)
        {
            return false;
        }
    }

    if !destination.from_domains.is_empty() {
        let Some(from_domain) = &event.from_domain else {
            return false;
        };

        if !destination
            .from_domains
            .iter()
            .any(|domain| domain == from_domain)
        {
            return false;
        }
    }

    if !destination.to_domains.is_empty()
        && !event.to_domains.iter().any(|domain| {
            destination
                .to_domains
                .iter()
                .any(|configured| configured == domain)
        })
    {
        return false;
    }

    true
}

fn normalize_destination(mut destination: DestinationConfig) -> DestinationConfig {
    destination.from_domains = destination
        .from_domains
        .into_iter()
        .filter_map(|domain| normalize_domain(&domain))
        .collect();
    destination.to_domains = destination
        .to_domains
        .into_iter()
        .filter_map(|domain| normalize_domain(&domain))
        .collect();
    destination
}

fn normalize_domain(domain: &str) -> Option<String> {
    let domain = domain
        .trim()
        .trim_start_matches('@')
        .trim_end_matches('.')
        .to_ascii_lowercase();

    if domain.is_empty() {
        None
    } else {
        Some(domain)
    }
}

#[cfg(test)]
mod tests {
    use super::RouteMatcher;
    use crate::{config::DestinationConfig, resend::ResendEventForRouting};

    #[test]
    fn matches_from_domain() {
        let matcher = RouteMatcher::new(vec![DestinationConfig {
            name: "app".to_string(),
            url: "https://app.example.com/webhooks/resend".to_string(),
            from_domains: vec!["Example.COM".to_string()],
            to_domains: vec![],
            event_types: vec![],
            catch_all: false,
        }]);
        let event = ResendEventForRouting {
            event_id: Some("email_123".to_string()),
            event_type: Some("email.delivered".to_string()),
            from_domain: Some("example.com".to_string()),
            to_domains: vec!["customer.com".to_string()],
        };

        let matched = matcher.match_destinations(&event);

        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].name, "app");
    }

    #[test]
    fn event_type_is_an_additional_constraint() {
        let matcher = RouteMatcher::new(vec![DestinationConfig {
            name: "app".to_string(),
            url: "https://app.example.com/webhooks/resend".to_string(),
            from_domains: vec!["example.com".to_string()],
            to_domains: vec![],
            event_types: vec!["email.bounced".to_string()],
            catch_all: false,
        }]);
        let event = ResendEventForRouting {
            event_id: Some("email_123".to_string()),
            event_type: Some("email.delivered".to_string()),
            from_domain: Some("example.com".to_string()),
            to_domains: vec![],
        };

        assert!(matcher.match_destinations(&event).is_empty());
    }
}
