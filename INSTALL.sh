#!/bin/sh

# POSIX sh installer for dtabnk
# Installs dtabnk.py -> /opt/dtabnk and wrapper -> /usr/local/bin/dtabnk

set -eu

SCRIPT_NAME="dtabnk.py"
SCRIPT_BASE="${SCRIPT_NAME%.py}"
COMMAND_NAME="dtabnk"
INSTALL_DIR="/opt/dtabnk"
WRAPPER_DIR="/usr/local/bin"

# run_elev: execute either with escalation command or directly (if running as root)
run_elev() {
    if [ -n "${ELEVATE_CMD:-}" ]; then
        # shellcheck disable=SC2086
        $ELEVATE_CMD "$@"
    else
        "$@"
    fi
}

# detect doas or sudo (prefer doas)
if command -v doas >/dev/null 2>&1; then
    ELEVATE_CMD="doas"
    printf '%s\n' "Using doas for privilege escalation."
elif command -v sudo >/dev/null 2>&1; then
    ELEVATE_CMD="sudo"
    printf '%s\n' "Using sudo for privilege escalation."
else
    # no doas/sudo: check if running as root
    if [ "$(id -u)" -eq 0 ]; then
        ELEVATE_CMD=""
        printf '%s\n' "No doas/sudo found but running as root; continuing without elevation."
    else
        printf '%s' "Neither sudo nor doas found. If you will run this as root press Enter; otherwise enter the command to use for privilege escalation: "
        # POSIX read
        if ! IFS= read -r user_cmd; then
            printf '%s\n' "Input error; aborting."
            exit 1
        fi
        # trim leading/trailing spaces (uses sed)
        user_cmd=$(printf '%s' "$user_cmd" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        if [ -z "$user_cmd" ]; then
            printf '%s\n' "No escalation command provided and not running as root. Aborting."
            exit 1
        fi
        # validate the user-provided command exists
        if ! command -v "$user_cmd" >/dev/null 2>&1; then
            printf '%s\n' "Provided escalation command '$user_cmd' not found in PATH. Aborting."
            exit 1
        fi
        ELEVATE_CMD="$user_cmd"
        printf '%s\n' "Using '$ELEVATE_CMD' for privilege escalation."
    fi
fi

# check for python3 or python
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    printf '%s\n' "Python 3 is not installed. Please install Python 3 and retry."
    exit 1
fi
printf '%s\n' "$PYTHON_CMD is available."

# verify script exists in current directory
if [ ! -f "$SCRIPT_NAME" ]; then
    printf '%s\n' "Error: $SCRIPT_NAME does not exist in the current directory."
    exit 1
fi

# create installation directory if needed
if ! run_elev test -d "$INSTALL_DIR"; then
    printf '%s\n' "Creating installation directory at $INSTALL_DIR..."
    run_elev mkdir -p "$INSTALL_DIR"
else
    printf '%s\n' "$INSTALL_DIR already exists."
fi

# copy python script
printf '%s\n' "Installing $SCRIPT_NAME to $INSTALL_DIR..."
run_elev cp "$SCRIPT_NAME" "$INSTALL_DIR/$SCRIPT_BASE"

# make executable
run_elev chmod +x "$INSTALL_DIR/$(basename "$SCRIPT_BASE")"

# ensure wrapper dir exists
if ! run_elev test -d "$WRAPPER_DIR"; then
    printf '%s\n' "Creating wrapper directory at $WRAPPER_DIR..."
    run_elev mkdir -p "$WRAPPER_DIR"
fi

# create temporary wrapper file (mktemp preferred)
if command -v mktemp >/dev/null 2>&1; then
    TMP_WRAPPER=$(mktemp /tmp/${COMMAND_NAME}.XXXXXX) || {
        printf '%s\n' "mktemp failed"; exit 1
    }
else
    TMP_WRAPPER="/tmp/${COMMAND_NAME}.$$"
fi

# write wrapper (use /bin/sh for portability)
cat > "$TMP_WRAPPER" <<EOF
#!/bin/sh
exec $INSTALL_DIR/$(basename "$SCRIPT_BASE") "\$@"
EOF

chmod +x "$TMP_WRAPPER"

# move into place with elevation
printf '%s\n' "Installing wrapper to $WRAPPER_DIR/$COMMAND_NAME..."
run_elev mv "$TMP_WRAPPER" "$WRAPPER_DIR/$COMMAND_NAME"

# clean up leftover tmp file if present
if [ -f "$TMP_WRAPPER" ]; then
    rm -f "$TMP_WRAPPER"
fi

printf '%s\n' "$SCRIPT_NAME has been installed to $INSTALL_DIR and can now be executed as '$COMMAND_NAME'."
exit 0
