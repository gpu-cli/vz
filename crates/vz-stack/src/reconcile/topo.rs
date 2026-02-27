use super::*;

pub(super) fn topo_sort(actions: &[Action], deps: &HashMap<&str, &[String]>) -> Vec<Action> {
    // Partition into creates and removes.
    let mut creates: Vec<&Action> = Vec::new();
    let mut removes: Vec<&Action> = Vec::new();

    for action in actions {
        match action {
            Action::ServiceCreate { .. } | Action::ServiceRecreate { .. } => {
                creates.push(action);
            }
            Action::ServiceRemove { .. } => {
                removes.push(action);
            }
        }
    }

    // Topological sort for creates: dependencies first.
    let create_names: HashSet<&str> = creates.iter().map(|a| a.service_name()).collect();
    let sorted_creates = topo_sort_names(&creates, deps, &create_names, false);

    // Topological sort for removes: dependents first (reverse dependency order).
    let remove_names: HashSet<&str> = removes.iter().map(|a| a.service_name()).collect();
    let sorted_removes = topo_sort_names(&removes, deps, &remove_names, true);

    // Creates first, then removes.
    let action_map: HashMap<&str, &Action> =
        actions.iter().map(|a| (a.service_name(), a)).collect();

    let mut result = Vec::new();
    for name in sorted_creates {
        if let Some(action) = action_map.get(name.as_str()) {
            result.push((*action).clone());
        }
    }
    for name in sorted_removes {
        if let Some(action) = action_map.get(name.as_str()) {
            result.push((*action).clone());
        }
    }

    result
}

/// Kahn's algorithm for topological sort with name-based tie-breaking.
///
/// When `reverse` is true, returns dependents before dependencies
/// (useful for teardown ordering).
fn topo_sort_names(
    actions: &[&Action],
    deps: &HashMap<&str, &[String]>,
    action_set: &HashSet<&str>,
    reverse: bool,
) -> Vec<String> {
    let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();

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

    result
}
