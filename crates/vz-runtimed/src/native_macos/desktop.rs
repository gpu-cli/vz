//! The logged-in desktop a native macOS Machine boots into.
//!
//! A macOS Machine is only useful for GUI work if its user has an Aqua session:
//! `launchctl print gui/<uid>` exists, WindowServer has a client, and an app
//! started with `launchctl asuser <uid> open -a …` can draw. Nothing in a
//! Machine's own lifecycle creates that session; loginwindow does, at boot,
//! when `autoLoginUser` names a user whose password `/etc/kcpassword` holds.
//!
//! Template setup writes both (`vz_macos_provision::apply_auto_config`) and
//! then sets `autoLoginUser` again from inside the running guest before the
//! template is sealed, so a Machine cloned from a current template logs in on
//! its first boot. A template prepared before that has the credential but not
//! the preference, so its Machines boot to loginwindow; for those, readiness
//! sets the preference and restarts the guest once, after which the Machine's
//! own disk carries it and every later boot logs straight in.
//!
//! The guest agent's own install is unchanged: it stays a root LaunchDaemon in
//! the system domain, and reaches the session with `launchctl asuser`.

/// The Machine user every native template creates (`UserConfig::default`).
pub(crate) const USER: &str = "dev";
pub(crate) const UID: u32 = 501;

/// What the probe found, one word on stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionState {
    /// `gui/<uid>` exists: the user is logged in.
    LoggedIn,
    /// Auto-login is configured and the session has not appeared yet; the
    /// guest is still booting into it.
    Pending,
    /// Auto-login was not configured and the probe has just configured it; it
    /// takes effect at the next boot.
    Configured,
}

impl SessionState {
    pub(crate) fn parse(stdout: &str) -> Option<Self> {
        match stdout.trim_end() {
            "logged-in" => Some(Self::LoggedIn),
            "pending" => Some(Self::Pending),
            "configured" => Some(Self::Configured),
            _ => None,
        }
    }
}

/// The one guest program readiness runs before deciding anything.
///
/// It refuses rather than configures when the credential is missing: setting
/// `autoLoginUser` without `/etc/kcpassword` makes loginwindow attempt a login
/// that cannot succeed, and restarting into that would cost a boot and still
/// leave no session.
pub(crate) fn probe_program() -> String {
    format!(
        "set -eu\n\
         if /bin/launchctl print gui/{UID} >/dev/null 2>&1; then echo logged-in; exit 0; fi\n\
         [ -s /etc/kcpassword ] || {{ echo 'no auto-login credential at /etc/kcpassword' >&2; exit 1; }}\n\
         current=$(/usr/bin/defaults read /Library/Preferences/com.apple.loginwindow autoLoginUser 2>/dev/null || true)\n\
         if [ \"$current\" = '{USER}' ]; then echo pending; exit 0; fi\n\
         /usr/bin/defaults write /Library/Preferences/com.apple.loginwindow autoLoginUser -string '{USER}'\n\
         echo configured\n"
    )
}

/// Exits zero once the user's session exists.
pub(crate) fn session_program() -> String {
    format!("/bin/launchctl print gui/{UID} >/dev/null 2>&1")
}

/// Prints the guest's boot time, so a restart can be told apart from an agent
/// that merely answered again.
pub(crate) const BOOT_TIME_PROGRAM: &str = "/usr/sbin/sysctl -n kern.boottime";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_probe_outcome_parses_and_nothing_else_does() {
        assert_eq!(
            SessionState::parse("logged-in\n"),
            Some(SessionState::LoggedIn)
        );
        assert_eq!(
            SessionState::parse("pending\n"),
            Some(SessionState::Pending)
        );
        assert_eq!(
            SessionState::parse("configured\n"),
            Some(SessionState::Configured)
        );
        assert_eq!(SessionState::parse(""), None);
        assert_eq!(SessionState::parse("logged-in\nextra\n"), None);
    }

    /// Run the probe's decision logic on this host, which is also macOS, with
    /// the guest's paths swapped for ones the test owns. It must not configure
    /// anything when the credential is absent.
    #[test]
    fn the_probe_refuses_without_a_credential_and_configures_with_one() {
        let directory = tempfile::tempdir().unwrap_or_else(|e| unreachable!("{e}"));
        let kcpassword = directory.path().join("kcpassword");
        let preferences = directory.path().join("com.apple.loginwindow");
        let program = probe_program()
            // An unused uid, so the session check fails on any host.
            .replace(&format!("gui/{UID}"), "gui/4242424")
            .replace("/etc/kcpassword", &kcpassword.to_string_lossy())
            .replace(
                "/Library/Preferences/com.apple.loginwindow",
                &preferences.to_string_lossy(),
            );
        let run = || {
            std::process::Command::new("/bin/sh")
                .args(["-c", &program])
                .output()
                .unwrap_or_else(|e| unreachable!("{e}"))
        };

        let refused = run();
        assert!(!refused.status.success());
        assert!(
            !preferences.with_extension("plist").exists(),
            "nothing is configured without a credential"
        );

        std::fs::write(&kcpassword, b"x").unwrap_or_else(|e| unreachable!("{e}"));
        let first = run();
        assert!(first.status.success());
        assert_eq!(
            SessionState::parse(&String::from_utf8_lossy(&first.stdout)),
            Some(SessionState::Configured)
        );
        let second = run();
        assert_eq!(
            SessionState::parse(&String::from_utf8_lossy(&second.stdout)),
            Some(SessionState::Pending),
            "a configured preference is not configured again"
        );
    }
}
