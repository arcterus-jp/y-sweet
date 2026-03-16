use anyhow::anyhow;
use axum::{
    extract::{Path, State},
    http::{header::HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use axum_extra::typed_header::TypedHeader;
use cuid::cuid2;
use std::sync::Arc;
use tracing::{error, info};
use y_sweet_core::{
    api_types::validate_doc_name,
    api_types_ext::{
        AssetUrl, AssetsResponse, ContentUploadRequest, ContentUploadResponse, DocCopyRequest,
        DocCopyResponse, DocDeleteResponse,
    },
    store::StoreError,
};

use crate::server::{get_authorization_from_plane_header, get_token_from_header, AppError, Server};

/// Check if the content type is allowed (only images and videos)
pub fn is_allowed_content_type(content_type: &str) -> bool {
    let mime = match content_type.parse::<mime::Mime>() {
        Ok(m) => m,
        Err(_) => return false,
    };

    // Check if it's an image or video
    let type_str = mime.type_().as_str();
    type_str == "image" || type_str == "video"
}

/// Get file extension from content type
pub fn get_extension_from_content_type(content_type: &str) -> String {
    let mime = content_type
        .parse::<mime::Mime>()
        .unwrap_or(mime::APPLICATION_OCTET_STREAM);
    let extension = mime_guess::get_mime_extensions(&mime)
        .and_then(|exts| exts.first())
        .unwrap_or(&"bin");
    format!(".{}", extension)
}

/// Extract asset ID from filename (without extension)
fn extract_asset_id_from_filename(filename: &str) -> Option<String> {
    // Find the last dot to separate asset_id and extension
    if let Some(last_dot_pos) = filename.rfind('.') {
        if last_dot_pos > 0 {
            return Some(filename[..last_dot_pos].to_string());
        }
    }
    // If no extension found, return the filename as is
    Some(filename.to_string())
}

/// Generate presigned URL for uploading content
async fn generate_upload_presigned_url(
    Path(doc_id): Path<String>,
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    Json(body): Json<ContentUploadRequest>,
) -> Result<Json<ContentUploadResponse>, AppError> {
    let token = get_token_from_header(auth_header);
    let _ = server_state.verify_doc_token(token.as_deref(), &doc_id)?;

    // Check if document exists
    if !server_state.doc_exists(&doc_id).await {
        Err((StatusCode::NOT_FOUND, anyhow!("Doc {} not found", doc_id)))?;
    }

    // Validate content type - only allow images and videos
    if !is_allowed_content_type(&body.content_type) {
        Err((
            StatusCode::BAD_REQUEST,
            anyhow!(
                "Content type '{}' is not allowed. Only image and video files are supported.",
                body.content_type
            ),
        ))?;
    }

    // Generate asset ID with cuid and extension
    let asset_id = cuid2();
    let extension = get_extension_from_content_type(&body.content_type);
    let asset_name = format!("{}{}", asset_id, extension);

    // Create the key path: {doc_id}/assets/{asset_name}
    let key = format!("{}/assets/{}", doc_id, asset_name);

    let upload_url = if let Some(store) = &server_state.store {
        store
            .generate_upload_presigned_url(&key, &body.content_type)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    anyhow!("Failed to generate upload URL: {:?}", e),
                )
            })?
    } else {
        // For local development without store, return a dummy URL
        format!("file://localhost/{}", key)
    };

    Ok(Json(ContentUploadResponse {
        upload_url,
        asset_id: asset_name,
    }))
}

/// Generate presigned URL for uploading content (single doc mode)
async fn generate_upload_presigned_url_single(
    State(server_state): State<Arc<Server>>,
    headers: HeaderMap,
    Json(body): Json<ContentUploadRequest>,
) -> Result<Json<ContentUploadResponse>, AppError> {
    let doc_id = server_state.get_single_doc_id()?;

    // the doc server is meant to be run in Plane, so we expect verified plane
    // headers to be used for authorization.
    let _ = get_authorization_from_plane_header(headers)?;

    // Validate content type - only allow images and videos
    if !is_allowed_content_type(&body.content_type) {
        Err((
            StatusCode::BAD_REQUEST,
            anyhow!(
                "Content type '{}' is not allowed. Only image and video files are supported.",
                body.content_type
            ),
        ))?;
    }

    // Generate asset ID with cuid and extension
    let asset_id = cuid2();
    let extension = get_extension_from_content_type(&body.content_type);
    let asset_name = format!("{}{}", asset_id, extension);

    // Create the key path: {doc_id}/assets/{asset_name}
    let key = format!("{}/assets/{}", doc_id, asset_name);

    let upload_url = if let Some(store) = &server_state.store {
        store
            .generate_upload_presigned_url(&key, &body.content_type)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    anyhow!("Failed to generate upload URL: {:?}", e),
                )
            })?
    } else {
        // For local development without store, return a dummy URL
        format!("file://localhost/{}", key)
    };

    Ok(Json(ContentUploadResponse {
        upload_url,
        asset_id: asset_name,
    }))
}

/// Get all assets for a document with presigned download URLs
async fn get_doc_assets(
    Path(doc_id): Path<String>,
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<impl IntoResponse, AppError> {
    let token = get_token_from_header(auth_header);
    let _ = server_state.verify_doc_token(token.as_deref(), &doc_id)?;

    // Check if document exists
    if !server_state.doc_exists(&doc_id).await {
        Err((StatusCode::NOT_FOUND, anyhow!("Doc {} not found", doc_id)))?;
    }

    let assets = if let Some(store) = &server_state.store {
        // List assets in the assets directory
        let assets_prefix = format!("{}/assets/", doc_id);
        let asset_names = store.list_objects(&assets_prefix).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("Failed to list assets: {:?}", e),
            )
        })?;

        // Generate signed URLs for each asset
        let mut asset_urls = Vec::new();
        for filename in asset_names {
            // Extract asset_id from filename (remove extension)
            if let Some(asset_id) = extract_asset_id_from_filename(&filename) {
                let key = format!("{}/assets/{}", doc_id, filename);
                let download_url =
                    store
                        .generate_download_presigned_url(&key)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                anyhow!(
                                    "Failed to generate download URL for {}: {:?}",
                                    filename,
                                    e
                                ),
                            )
                        })?;

                asset_urls.push(AssetUrl {
                    asset_id,
                    download_url,
                });
            }
        }

        asset_urls
    } else {
        // For local development without store, return empty list
        Vec::new()
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=30"),
    );
    Ok((headers, Json(AssetsResponse { assets })))
}

/// Get all assets for a document (single doc mode)
async fn get_doc_assets_single(
    State(server_state): State<Arc<Server>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let doc_id = server_state.get_single_doc_id()?;
    let _authorization = get_authorization_from_plane_header(headers)?;

    if let Some(store) = &server_state.store {
        let mut assets = Vec::new();

        // List all objects in the document's assets directory
        let assets_prefix = format!("{}/assets/", doc_id);
        let objects = store.list_objects(&assets_prefix).await.map_err(|e| {
            AppError(
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("Failed to list assets: {}", e),
            )
        })?;

        for object_key in objects {
            // Extract asset ID from the object key
            if let Some(asset_id) = extract_asset_id_from_filename(&object_key) {
                let download_url = store
                    .generate_download_presigned_url(&object_key)
                    .await
                    .map_err(|e| {
                        AppError(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            anyhow!("Failed to generate download URL: {}", e),
                        )
                    })?;

                assets.push(AssetUrl {
                    asset_id,
                    download_url,
                });
            }
        }

        let mut resp_headers = HeaderMap::new();
        resp_headers.insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("private, max-age=30"),
        );
        Ok((resp_headers, Json(AssetsResponse { assets })))
    } else {
        Err(AppError(
            StatusCode::INTERNAL_SERVER_ERROR,
            anyhow!("No store configured"),
        ))
    }
}

/// Delete a document and all associated assets
pub async fn delete_document(
    Path(doc_id): Path<String>,
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<Json<DocDeleteResponse>, AppError> {
    // Check authentication - this is an admin-only API
    server_state.check_auth(auth_header)?;

    if !validate_doc_name(&doc_id) {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid document ID"),
        ));
    }

    if !server_state.doc_exists(&doc_id).await {
        return Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow!("Document not found"),
        ));
    }

    info!(
        message = "Deleting document",
        event = "document_delete_started",
        doc_id = %doc_id
    );

    let mut existed_in_memory = false;
    if let Some((_, doc)) = server_state.docs.remove(&doc_id) {
        existed_in_memory = true;
        // Shut down persistence to avoid resurrecting the document
        doc.sync_kv().shutdown();
    }

    let mut data_deleted = false;
    let mut deleted_assets = 0usize;

    if let Some(store) = &server_state.store {
        let data_key = format!("{}/data.ysweet", doc_id);
        match store.remove(&data_key).await {
            Ok(_) => {
                data_deleted = true;
            }
            Err(StoreError::DoesNotExist(_)) => {}
            Err(e) => {
                error!(
                    message = "Failed to delete document data",
                    event = "document_delete_failed",
                    doc_id = %doc_id,
                    error = %e
                );
                return Err(AppError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    anyhow!("Failed to delete document data: {}", e),
                ));
            }
        }

        let assets_prefix = format!("{}/assets/", doc_id);
        match store.list_objects(&assets_prefix).await {
            Ok(asset_names) => {
                for filename in asset_names {
                    let key = format!("{}/assets/{}", doc_id, filename);
                    match store.remove(&key).await {
                        Ok(_) => {
                            deleted_assets += 1;
                        }
                        Err(StoreError::DoesNotExist(_)) => {}
                        Err(e) => {
                            error!(
                                message = "Failed to delete document asset",
                                event = "document_delete_asset_failed",
                                doc_id = %doc_id,
                                asset = %filename,
                                error = %e
                            );
                            return Err(AppError(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                anyhow!("Failed to delete asset {}: {}", filename, e),
                            ));
                        }
                    }
                }
            }
            Err(StoreError::DoesNotExist(_)) => {}
            Err(e) => {
                error!(
                    message = "Failed to list document assets",
                    event = "document_delete_failed",
                    doc_id = %doc_id,
                    error = %e
                );
                return Err(AppError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    anyhow!("Failed to list assets for deletion: {}", e),
                ));
            }
        }
    }

    let success = existed_in_memory || data_deleted || deleted_assets > 0;

    info!(
        message = "Document deleted",
        event = "document_delete_completed",
        doc_id = %doc_id,
        data_deleted = data_deleted,
        deleted_assets = deleted_assets,
        existed_in_memory = existed_in_memory
    );

    Ok(Json(DocDeleteResponse {
        doc_id,
        success,
        data_deleted,
        deleted_assets,
    }))
}

/// Copy a document to a new document ID
pub async fn copy_document(
    Path(source_doc_id): Path<String>,
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    Json(body): Json<DocCopyRequest>,
) -> Result<Json<DocCopyResponse>, AppError> {
    // Check authentication - this is an admin-only API
    server_state.check_auth(auth_header)?;

    let destination_doc_id = body.destination_doc_id;

    // Validate document IDs
    if !validate_doc_name(&source_doc_id) {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid source document ID"),
        ));
    }

    if !validate_doc_name(&destination_doc_id) {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid destination document ID"),
        ));
    }

    // Check if source document exists
    if !server_state.doc_exists(&source_doc_id).await {
        return Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow!("Source document not found"),
        ));
    }

    // Force sync from memory to S3 before copying to ensure we have the latest data
    if let Some(doc) = server_state.docs.get(&source_doc_id) {
        tracing::debug!(
            "Forcing sync of source document {} before copy",
            source_doc_id
        );
        if let Err(e) = doc.sync_kv().persist().await {
            tracing::warn!(
                "Failed to force sync of source document {}: {}",
                source_doc_id,
                e
            );
            // Continue with copy operation even if sync fails
        }
    }

    // Perform the copy operation (will overwrite if destination exists)
    if let Some(store) = &server_state.store {
        store
            .copy_document(&source_doc_id, &destination_doc_id)
            .await
            .map_err(|e| {
                AppError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    anyhow!("Failed to copy document: {}", e),
                )
            })?;

        Ok(Json(DocCopyResponse {
            source_doc_id,
            destination_doc_id,
            success: true,
        }))
    } else {
        Err(AppError(
            StatusCode::INTERNAL_SERVER_ERROR,
            anyhow!("No store configured"),
        ))
    }
}

/// Extension routes for custom endpoints
pub fn ext_routes(server: &Arc<Server>) -> Router {
    Router::new()
        .route("/d/:doc_id", delete(delete_document))
        .route("/d/:doc_id/copy", post(copy_document))
        .route("/d/:doc_id/assets", post(generate_upload_presigned_url))
        .route("/d/:doc_id/assets", get(get_doc_assets))
        .with_state(server.clone())
}

/// Extension routes for custom endpoints (single doc mode)
pub fn ext_single_doc_routes(server: &Arc<Server>) -> Router {
    Router::new()
        .route("/assets", post(generate_upload_presigned_url_single))
        .route("/assets", get(get_doc_assets_single))
        .with_state(server.clone())
}
