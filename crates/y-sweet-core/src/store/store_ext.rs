use super::{Result, Store};
use async_trait::async_trait;

/// Custom Store extension functionality
///
/// Provides extension features for S3 and filesystem stores such as
/// presigned URL generation, object listing, and document copying.
#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait StoreExt: Store {
    /// アップロード用の署名付きURLを生成します
    async fn generate_upload_presigned_url(&self, key: &str, content_type: &str) -> Result<String>;

    /// ダウンロード用の署名付きURLを生成します
    async fn generate_download_presigned_url(&self, key: &str) -> Result<String>;

    /// 指定されたプレフィックスに一致するオブジェクトのリストを取得します
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;

    /// ドキュメントを別のドキュメントIDにコピーします
    async fn copy_document(&self, source_doc_id: &str, destination_doc_id: &str) -> Result<()>;
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait StoreExt: Store + Send + Sync {
    /// アップロード用の署名付きURLを生成します
    async fn generate_upload_presigned_url(&self, key: &str, content_type: &str) -> Result<String>;

    /// ダウンロード用の署名付きURLを生成します
    async fn generate_download_presigned_url(&self, key: &str) -> Result<String>;

    /// 指定されたプレフィックスに一致するオブジェクトのリストを取得します
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;

    /// ドキュメントを別のドキュメントIDにコピーします
    async fn copy_document(&self, source_doc_id: &str, destination_doc_id: &str) -> Result<()>;
}
