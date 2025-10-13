# Mnemosyne

## Miscellaneous notes

#### Running poetry 

Get python info using `poetry env info --path`. Install `poetry add ipykernel`. Check kernel path by `poetry env info`. 

Installing type annotations for libraries: `poetry add --group dev types-requests`

Commands:

- `poetry env info`

#### Viewing and removing poetry artifacts

View artifacts:
```bash
poetry env list          # List all Poetry environments
poetry env info          # Show current environment details
jupyter kernelspec list  # List Jupyter kernels
```

Remove artifacts:
```bash
poetry env remove <env-name>              # Remove Poetry environment
jupyter kernelspec uninstall <name> -y   # Remove Jupyter kernel
rm -f poetry.lock                         # Remove lock file
```

# Memory pressure kill
 Solutions (in order of preference)

  1. Disable systemd-oomd for your session (quickest fix)

  sudo systemctl stop systemd-oomd
  sudo systemctl disable systemd-oomd

  Then restart your session and try again. This removes the aggressive killer.

  2. Run job in a system slice (bypass user session limits)

  sudo systemd-run --uid=$(id -u) --gid=$(id -g) --working-directory=$PWD \
    --slice=system.slice \
    jupyter lab

  This runs Jupyter outside your user session, so systemd-oomd won't kill it.

  3. Increase systemd-oomd thresholds (edit /etc/systemd/oomd.conf)

  [OOM]
  DefaultMemoryPressureLimit=80%   # Default is often 60%
  DefaultMemoryPressureDurationSec=60s  # Give more time before killing
