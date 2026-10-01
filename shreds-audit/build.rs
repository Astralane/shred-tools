use std::process::Command;

fn main() {
    println!("cargo:rustc-env=GIT_COMMIT={}", git_commit());

    if let Some(git_dir) = run_git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        if let Some(head_ref) = run_git(&["symbolic-ref", "--quiet", "HEAD"]) {
            println!("cargo:rerun-if-changed={git_dir}/{head_ref}");
            println!("cargo:rerun-if-changed={git_dir}/packed-refs");
        }
    }
}

fn git_commit() -> String {
    let Some(hash) = run_git(&["rev-parse", "--short", "HEAD"]) else {
        return "unknown".to_string();
    };
    match run_git(&["status", "--porcelain"]) {
        Some(s) if !s.is_empty() => format!("{hash}-dirty"),
        _ => hash,
    }
}

fn run_git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8(out.stdout).ok()?.trim().to_string())
}
