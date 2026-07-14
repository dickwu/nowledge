use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Path, PathBuf},
    process,
};

use anyhow::{anyhow, bail, Context, Result};
use nowledge::{
    meili::MeiliAdmin,
    tenant_scope_v1::{
        apply_plan, create_plan, create_rollback_plan, plan_report, read_json, verify_plan,
        write_json_atomic, FileCheckpointStore, LegacyTenantMapping, MigrationPlan, RollbackPlan,
        DEFAULT_BATCH_SIZE, MIGRATION_NAME,
    },
    Config,
};
use serde::Serialize;
use serde_json::{json, Value};

#[tokio::main]
async fn main() {
    match run().await {
        Ok(report) => match serde_json::to_string_pretty(&report) {
            Ok(encoded) => println!("{encoded}"),
            Err(error) => fail(anyhow!(error)),
        },
        Err(error) => fail(error),
    }
}

async fn run() -> Result<Value> {
    let command = Command::parse(env::args().skip(1))?;
    if command.mode == "help" {
        return Ok(json!({
            "mode": "help",
            "migration": MIGRATION_NAME,
            "usage": usage()
        }));
    }

    match command.mode.as_str() {
        "plan" => {
            command.require_only(&["mapping", "out", "batch-size"])?;
            let mapping_path = command.required_path("mapping")?;
            let output_path = command.required_path("out")?;
            let batch_size = command
                .optional("batch-size")
                .map(str::parse::<usize>)
                .transpose()
                .context("--batch-size must be an integer")?
                .unwrap_or(DEFAULT_BATCH_SIZE);
            let admin = configured_admin()?;
            let mapping: LegacyTenantMapping = read_json(&mapping_path)?;
            let plan = create_plan(&admin, &mapping, batch_size).await?;
            write_json_atomic(&output_path, &plan)?;
            Ok(json!({
                "artifact": output_path,
                "report": plan_report(&plan)
            }))
        }
        "rollback-plan" => {
            command.require_only(&["plan", "out"])?;
            let plan_path = command.required_path("plan")?;
            let output_path = command.required_path("out")?;
            let plan: MigrationPlan = read_json(&plan_path)?;
            let rollback = create_rollback_plan(&plan)?;
            write_json_atomic(&output_path, &rollback)?;
            Ok(json!({
                "mode": "rollback-plan",
                "migration": MIGRATION_NAME,
                "artifact": output_path,
                "plan_checksum": rollback.plan_checksum,
                "rollback_checksum": rollback.rollback_checksum,
                "action_count": rollback.actions.len(),
                "preserves_legacy_rows": rollback.preserves_legacy_rows,
                "acknowledgement": rollback.acknowledgement
            }))
        }
        "apply" => {
            command.require_only(&["plan", "rollback-plan", "ack", "checkpoint", "dry-run"])?;
            let plan_path = command.required_path("plan")?;
            let rollback_path = command.required_path("rollback-plan")?;
            let checkpoint_path = command.required_path("checkpoint")?;
            let acknowledgement = command.required("ack")?;
            let plan: MigrationPlan = read_json(&plan_path)?;
            let rollback: RollbackPlan = read_json(&rollback_path)?;
            let admin = configured_admin()?;
            let mut checkpoints = FileCheckpointStore::new(checkpoint_path.clone());
            let report = apply_plan(
                &admin,
                &plan,
                &rollback,
                acknowledgement,
                &mut checkpoints,
                command.flag("dry-run"),
            )
            .await?;
            Ok(json!({
                "checkpoint": checkpoint_path,
                "report": report
            }))
        }
        "verify" => {
            command.require_only(&["plan"])?;
            let plan_path = command.required_path("plan")?;
            let plan: MigrationPlan = read_json(&plan_path)?;
            let admin = configured_admin()?;
            let report = verify_plan(&admin, &plan).await?;
            Ok(serde_json::to_value(report)?)
        }
        other => bail!("unknown mode {other}; expected plan, apply, verify, or rollback-plan"),
    }
}

fn configured_admin() -> Result<MeiliAdmin> {
    let admin = MeiliAdmin::from_config(&Config::from_env());
    if !admin.configured() {
        bail!("RAG_MEILI_URL is required for tenant_scope_v1 maintenance");
    }
    Ok(admin)
}

#[derive(Debug)]
struct Command {
    mode: String,
    values: BTreeMap<String, String>,
    flags: BTreeSet<String>,
}

impl Command {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self> {
        let arguments = arguments.into_iter().collect::<Vec<_>>();
        if arguments.is_empty() || matches!(arguments[0].as_str(), "help" | "--help" | "-h") {
            return Ok(Self {
                mode: "help".to_string(),
                values: BTreeMap::new(),
                flags: BTreeSet::new(),
            });
        }
        let mode = arguments[0].clone();
        let mut values = BTreeMap::new();
        let mut flags = BTreeSet::new();
        let mut index = 1;
        while index < arguments.len() {
            let argument = &arguments[index];
            let Some(name) = argument.strip_prefix("--") else {
                bail!("unexpected positional argument {argument}");
            };
            if name.is_empty() {
                bail!("empty option name");
            }
            if name == "dry-run" {
                if !flags.insert(name.to_string()) {
                    bail!("duplicate option --{name}");
                }
                index += 1;
                continue;
            }
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| anyhow!("option --{name} requires a value"))?;
            if value.starts_with("--") {
                bail!("option --{name} requires a value");
            }
            if values.insert(name.to_string(), value.clone()).is_some() {
                bail!("duplicate option --{name}");
            }
            index += 2;
        }
        Ok(Self {
            mode,
            values,
            flags,
        })
    }

    fn require_only(&self, allowed: &[&str]) -> Result<()> {
        let allowed = allowed.iter().copied().collect::<BTreeSet<_>>();
        for name in self.values.keys().chain(self.flags.iter()) {
            if !allowed.contains(name.as_str()) {
                bail!("option --{name} is not valid for mode {}", self.mode);
            }
        }
        Ok(())
    }

    fn required(&self, name: &str) -> Result<&str> {
        self.optional(name)
            .ok_or_else(|| anyhow!("mode {} requires --{name}", self.mode))
    }

    fn optional(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    fn required_path(&self, name: &str) -> Result<PathBuf> {
        self.required(name).map(Path::new).map(Path::to_path_buf)
    }

    fn flag(&self, name: &str) -> bool {
        self.flags.contains(name)
    }
}

fn usage() -> &'static str {
    "tenant_scope_v1 plan --mapping MAPPING.json --out PLAN.json [--batch-size 250]\n\
tenant_scope_v1 rollback-plan --plan PLAN.json --out ROLLBACK.json\n\
tenant_scope_v1 apply --plan PLAN.json --rollback-plan ROLLBACK.json --ack ACK --checkpoint CHECKPOINT.json [--dry-run]\n\
tenant_scope_v1 verify --plan PLAN.json"
}

fn fail(error: anyhow::Error) -> ! {
    #[derive(Serialize)]
    struct ErrorEnvelope<'a> {
        mode: &'static str,
        migration: &'static str,
        ok: bool,
        error: ErrorDetail<'a>,
    }

    #[derive(Serialize)]
    struct ErrorDetail<'a> {
        code: &'static str,
        message: &'a str,
    }

    let message = error.to_string();
    let envelope = ErrorEnvelope {
        mode: "error",
        migration: MIGRATION_NAME,
        ok: false,
        error: ErrorDetail {
            code: "tenant_scope_v1_failed",
            message: &message,
        },
    };
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&envelope).unwrap_or_else(|_| {
            "{\"mode\":\"error\",\"migration\":\"tenant_scope_v1\",\"ok\":false}".to_string()
        })
    );
    process::exit(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_apply_dry_run_without_treating_it_as_a_value_option() {
        let command = Command::parse([
            "apply".to_string(),
            "--plan".to_string(),
            "plan.json".to_string(),
            "--dry-run".to_string(),
        ])
        .unwrap();
        assert_eq!(command.mode, "apply");
        assert_eq!(command.required("plan").unwrap(), "plan.json");
        assert!(command.flag("dry-run"));
    }

    #[test]
    fn rejects_duplicate_and_unknown_options() {
        assert!(Command::parse([
            "verify".to_string(),
            "--plan".to_string(),
            "one.json".to_string(),
            "--plan".to_string(),
            "two.json".to_string(),
        ])
        .is_err());

        let command = Command::parse([
            "verify".to_string(),
            "--mapping".to_string(),
            "mapping.json".to_string(),
        ])
        .unwrap();
        assert!(command.require_only(&["plan"]).is_err());
    }
}
