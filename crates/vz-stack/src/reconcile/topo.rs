use super::*;

pub(super) fn topo_sort(
    actions: &[ActionDraft],
    deps: &HashMap<&str, &[String]>,
) -> Result<Vec<ActionDraft>, StackError> {
    // Partition into creates and removes.
    let mut creates: Vec<&ActionDraft> = Vec::new();
    let mut removes: Vec<&ActionDraft> = Vec::new();

    for action in actions {
        match action {
            ActionDraft::Create { .. } | ActionDraft::Recreate { .. } => {
                creates.push(action);
            }
            ActionDraft::Remove { .. } => {
                removes.push(action);
            }
        }
    }

    // Topological sort for creates: dependencies first.
    let create_names: HashSet<&str> = creates.iter().map(|a| a.service_name()).collect();
    let sorted_creates = topo_sort_names(deps, &create_names, false)?;

    // Topological sort for removes: dependents first (reverse dependency order).
    let remove_names: HashSet<&str> = removes.iter().map(|a| a.service_name()).collect();
    let sorted_removes = topo_sort_names(deps, &remove_names, true)?;

    let mut result = actions_for_service_order(&creates, &sorted_creates);
    result.extend(actions_for_service_order(&removes, &sorted_removes));

    if result.len() != actions.len() {
        return Err(StackError::InvalidSpec(
            "topological ordering lost exact replica actions".to_string(),
        ));
    }
    Ok(result)
}

fn actions_for_service_order(
    actions: &[&ActionDraft],
    service_order: &[String],
) -> Vec<ActionDraft> {
    let mut grouped: HashMap<&str, Vec<&ActionDraft>> = HashMap::new();
    for action in actions {
        grouped
            .entry(action.service_name())
            .or_default()
            .push(action);
    }
    for service_actions in grouped.values_mut() {
        service_actions.sort_by(|left, right| left.target().cmp(right.target()));
    }

    service_order
        .iter()
        .flat_map(|service_name| grouped.remove(service_name.as_str()).unwrap_or_default())
        .cloned()
        .collect()
}

/// Kahn's algorithm for topological sort with name-based tie-breaking.
///
/// When `reverse` is true, returns dependents before dependencies
/// (useful for teardown ordering).
fn topo_sort_names(
    deps: &HashMap<&str, &[String]>,
    action_set: &HashSet<&str>,
    reverse: bool,
) -> Result<Vec<String>, StackError> {
    let mut names: Vec<&str> = action_set.iter().copied().collect();
    names.sort_unstable();

    // Build in-degree map considering only actions in our set.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

    for &name in &names {
        in_degree.entry(name).or_insert(0);
        adj.entry(name).or_default();
    }

    for &name in &names {
        let dependencies = deps.get(name).copied().unwrap_or_default();
        for dep in dependencies {
            if action_set.contains(dep.as_str()) {
                if reverse {
                    // For teardown: dependent → dependency (dependent goes first).
                    *in_degree.entry(dep.as_str()).or_insert(0) += 1;
                    adj.entry(name).or_default().push(dep.as_str());
                } else {
                    // For startup: dependency → dependent (dependency goes first).
                    *in_degree.entry(name).or_insert(0) += 1;
                    adj.entry(dep.as_str()).or_default().push(name);
                }
            }
        }
    }

    // Kahn's algorithm with sorted queue for deterministic tie-breaking.
    let mut queue: VecDeque<&str> = VecDeque::new();
    let mut ready: Vec<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(name, _)| *name)
        .collect();
    ready.sort();
    queue.extend(ready);

    let mut result = Vec::new();
    while let Some(name) = queue.pop_front() {
        result.push(name.to_string());

        let neighbors: Vec<&str> = adj.get(name).cloned().unwrap_or_default();
        let mut newly_ready: Vec<&str> = Vec::new();

        for neighbor in neighbors {
            if let Some(deg) = in_degree.get_mut(neighbor) {
                *deg -= 1;
                if *deg == 0 {
                    newly_ready.push(neighbor);
                }
            }
        }

        newly_ready.sort();
        queue.extend(newly_ready);
    }

    if result.len() != names.len() {
        let mut cyclic = names
            .into_iter()
            .filter(|name| !result.iter().any(|ordered| ordered == name))
            .collect::<Vec<_>>();
        cyclic.sort_unstable();
        return Err(StackError::InvalidSpec(format!(
            "dependency cycle among planned services: {}",
            cyclic.join(", ")
        )));
    }

    Ok(result)
}
