use std::process::Command;
fn main() {
    println!("cargo:rerun-if-env-changed=SOURCE_REVISION");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=../binaries/src");
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs/heads/low-latency-ws");
    let revision = std::env::var("SOURCE_REVISION")
        .ok()
        .filter(|v| !v.is_empty() && v != "unknown")
        .or_else(|| {
            Command::new("git").args(["rev-parse", "HEAD"]).output().ok().filter(|out| out.status.success()).map(
                |out| {
                    let mut revision = String::from_utf8_lossy(&out.stdout).trim().to_owned();
                    if Command::new("git")
                        .args(["status", "--porcelain", "--untracked-files=normal"])
                        .output()
                        .is_ok_and(|status| status.status.success() && !status.stdout.is_empty())
                    {
                        revision.push_str("-dirty");
                    }
                    revision
                },
            )
        })
        .unwrap_or_else(|| "unknown (build with SOURCE_REVISION)".into());
    // Metadata is informational: runtime capabilities remain authoritative.
    println!("cargo:rustc-env=WS_SOURCE_REVISION={}", revision.replace(['\n', '\r'], ""));
}
