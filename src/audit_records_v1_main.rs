mod audit_records_v1;

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Path, PathBuf},
    process,
};

use anyhow::{anyhow, bail, Result};
use audit_records_v1::{
    apply_plan, create_plan, plan_report, verify_plan, MigrationPlan, MIGRATION_NAME,
};
use nowledge::{
    meili::MeiliAdmin,
    tenant_scope_v1::{read_json, write_json_atomic},
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
            "usage": usage(),
        }));
    }

    match command.mode.as_str() {
        "plan" => {
            command.require_only(&["out"])?;
            let output_path = command.required_path("out")?;
            let admin = configured_admin()?;
            let plan = create_plan(&admin).await?;
            write_json_atomic(&output_path, &plan)?;
            Ok(json!({
                "artifact": output_path,
                "report": plan_report(&plan),
            }))
        }
        "apply" => {
            command.require_only(&["plan", "dry-run"])?;
            let plan_path = command.required_path("plan")?;
            let plan: MigrationPlan = read_json(&plan_path)?;
            let admin = configured_admin()?;
            let report = apply_plan(&admin, &plan, command.flag("dry-run")).await?;
            Ok(serde_json::to_value(report)?)
        }
        "verify" => {
            command.require_only(&["plan"])?;
            let plan_path = command.required_path("plan")?;
            let plan: MigrationPlan = read_json(&plan_path)?;
            let admin = configured_admin()?;
            let report = verify_plan(&admin, &plan).await?;
            ensure_verification_ready(report.ready, &report.failures)?;
            Ok(serde_json::to_value(report)?)
        }
        other => bail!("unknown mode {other}; expected plan, apply, or verify"),
    }
}

fn ensure_verification_ready(ready: bool, failures: &[String]) -> Result<()> {
    if ready {
        return Ok(());
    }
    let reason = if failures.is_empty() {
        "verification did not reach ready state".to_string()
    } else {
        failures.join("; ")
    };
    bail!("audit_records_v1 verification failed: {reason}")
}

fn configured_admin() -> Result<MeiliAdmin> {
    let config = Config::from_env();
    config.validate_meili_maintenance()?;
    Ok(MeiliAdmin::from_admin_config(&config))
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
        self.values
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| anyhow!("mode {} requires --{name}", self.mode))
    }

    fn required_path(&self, name: &str) -> Result<PathBuf> {
        self.required(name).map(Path::new).map(Path::to_path_buf)
    }

    fn flag(&self, name: &str) -> bool {
        self.flags.contains(name)
    }
}

fn usage() -> &'static str {
    "audit_records_v1 plan --out PLAN.json\n\
audit_records_v1 apply --plan PLAN.json [--dry-run]\n\
audit_records_v1 verify --plan PLAN.json"
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
            code: "audit_records_v1_failed",
            message: &message,
        },
    };
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&envelope).unwrap_or_else(|_| {
            "{\"mode\":\"error\",\"migration\":\"audit_records_v1\",\"ok\":false}".to_string()
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
        command.require_only(&["plan", "dry-run"]).unwrap();
        assert_eq!(command.required("plan").unwrap(), "plan.json");
        assert!(command.flag("dry-run"));
    }

    #[test]
    fn rejects_duplicate_unknown_and_destructive_options() {
        assert!(Command::parse([
            "verify".to_string(),
            "--plan".to_string(),
            "one.json".to_string(),
            "--plan".to_string(),
            "two.json".to_string(),
        ])
        .is_err());

        for destructive in ["reset", "delete", "force"] {
            let command = Command::parse([
                "apply".to_string(),
                format!("--{destructive}"),
                "true".to_string(),
            ])
            .unwrap();
            assert!(command.require_only(&["plan", "dry-run"]).is_err());
        }
    }

    #[test]
    fn modes_require_only_their_documented_arguments() {
        let plan = Command::parse([
            "plan".to_string(),
            "--out".to_string(),
            "plan.json".to_string(),
        ])
        .unwrap();
        plan.require_only(&["out"]).unwrap();
        assert!(plan.required("out").is_ok());

        let verify = Command::parse(["verify".to_string()]).unwrap();
        verify.require_only(&["plan"]).unwrap();
        assert!(verify.required("plan").is_err());
    }

    #[test]
    fn failed_verification_is_an_error_for_deployment_automation() {
        ensure_verification_ready(true, &[]).unwrap();
        let error = ensure_verification_ready(
            false,
            &["required audit records index is missing".to_string()],
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("required audit records index is missing"));
    }
}
