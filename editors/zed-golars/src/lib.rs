use zed::settings::LspSettings;
use zed_extension_api::{self as zed, LanguageServerId, Result};

struct GolarsExtension;

impl GolarsExtension {
    fn language_server_binary_path(
        &mut self,
        _language_server_id: &LanguageServerId,
        worktree: &zed::Worktree,
    ) -> Result<String> {
        // Check for user-defined path in settings first
        if let Some(path) = LspSettings::for_worktree("golars-lsp", worktree)
            .ok()
            .and_then(|s| s.binary)
            .and_then(|b| b.path)
        {
            return Ok(path);
        }

        // Check if golars-lsp is available on PATH
        if let Some(path) = worktree.which("golars-lsp") {
            return Ok(path);
        }

        // Not found - return helpful error message
        Err("golars-lsp not found on PATH.

Install with:
    go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest

Or configure a custom path in Zed settings:
    \"lsp\": {
        \"golars-lsp\": {
            \"binary\": { \"path\": \"/path/to/golars-lsp\" }
        }
    }".into())
    }
}

impl zed::Extension for GolarsExtension {
    fn new() -> Self {
        Self
    }

    fn language_server_command(
        &mut self,
        language_server_id: &LanguageServerId,
        worktree: &zed::Worktree,
    ) -> Result<zed::Command> {
        Ok(zed::Command {
            command: self.language_server_binary_path(language_server_id, worktree)?,
            args: vec![],
            env: Default::default(),
        })
    }

    fn language_server_workspace_configuration(
        &mut self,
        _language_server_id: &LanguageServerId,
        _worktree: &zed::Worktree,
    ) -> Result<Option<zed::serde_json::Value>> {
        Ok(None)
    }
}

zed::register_extension!(GolarsExtension);
