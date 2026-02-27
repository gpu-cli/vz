use super::*;
pub(crate) async fn try_list_events_via_daemon(
    state: &ApiState,
    stack_name: &str,
    query: &EventsQuery,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let limit = query
        .limit
        .unwrap_or(state.default_event_page_size)
        .clamp(1, MAX_EVENT_PAGE_SIZE);
    let after = query.after.unwrap_or(0);

    match client
        .list_events(runtime_v2::ListEventsRequest {
            stack_name: stack_name.to_string(),
            after,
            limit: limit as u32,
            scope: query.scope.clone().unwrap_or_default(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let events = response
                .events
                .into_iter()
                .map(api_event_record_from_runtime_proto)
                .collect();
            Some(
                (
                    StatusCode::OK,
                    Json(EventsResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        events,
                        next_cursor: response.next_cursor,
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(crate) async fn open_event_stream_via_daemon(
    state: &ApiState,
    stack_name: &str,
    query: &EventsQuery,
    request_id: &str,
) -> Result<tonic::Streaming<runtime_v2::RuntimeEvent>, Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Err(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    client
        .stream_events(runtime_v2::StreamEventsRequest {
            stack_name: stack_name.to_string(),
            after: query.after.unwrap_or(0),
            scope: query.scope.clone().unwrap_or_default(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
        .map_err(|error| match error {
            DaemonClientError::Grpc(status) => daemon_status_to_http_response(status, request_id),
            other => json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &other.to_string(),
                request_id,
            ),
        })
}
