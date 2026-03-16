use serde::{Deserialize, Serialize};

/// Request for generating a presigned URL for content upload
#[derive(Deserialize)]
pub struct ContentUploadRequest {
    /// The content type of the file to upload
    #[serde(rename = "contentType")]
    pub content_type: String,
}

/// Response containing a presigned URL for content upload
#[derive(Serialize)]
pub struct ContentUploadResponse {
    /// The signed URL for uploading the content
    #[serde(rename = "uploadUrl")]
    pub upload_url: String,

    /// The asset ID that will be used to store the content
    #[serde(rename = "assetId")]
    pub asset_id: String,
}

/// Asset URL with presigned download URL
#[derive(Serialize)]
pub struct AssetUrl {
    /// The asset ID (without extension) of the asset
    #[serde(rename = "assetId")]
    pub asset_id: String,

    /// The signed URL for downloading the asset
    #[serde(rename = "downloadUrl")]
    pub download_url: String,
}

/// Response containing a list of assets with presigned download URLs
#[derive(Serialize)]
pub struct AssetsResponse {
    /// List of asset URLs with signed download URLs
    pub assets: Vec<AssetUrl>,
}

/// Request for copying a document to a new document ID
#[derive(Deserialize)]
pub struct DocCopyRequest {
    /// The ID of the destination document where the source document will be copied to
    #[serde(rename = "destinationDocId")]
    pub destination_doc_id: String,
}

/// Response for document copy operation
#[derive(Serialize)]
pub struct DocCopyResponse {
    /// The ID of the source document that was copied
    #[serde(rename = "sourceDocId")]
    pub source_doc_id: String,
    /// The ID of the destination document where the copy was created
    #[serde(rename = "destinationDocId")]
    pub destination_doc_id: String,
    /// Whether the copy operation was successful
    pub success: bool,
}

/// Response for document deletion operation
#[derive(Serialize)]
pub struct DocDeleteResponse {
    /// The document that was deleted.
    #[serde(rename = "docId")]
    pub doc_id: String,
    /// Whether the stored snapshot (data.ysweet) was removed.
    #[serde(rename = "dataDeleted")]
    pub data_deleted: bool,
    /// Number of asset objects removed from storage.
    #[serde(rename = "deletedAssets")]
    pub deleted_assets: usize,
    /// Indicates that the delete operation completed without errors.
    pub success: bool,
}
