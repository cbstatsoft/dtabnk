# Security Policy

## Supported Versions

Security fixes are applied to the current maintained version of `dtabnk`.

Older releases may not receive security updates. Users are encouraged to update to the latest available version before reporting an issue.

## Reporting a Vulnerability

Please report suspected security vulnerabilities privately.

Do **not** open a public issue containing:

- exploit details
- proof-of-concept code that could be abused
- credentials, tokens, or other secrets
- sensitive user or dataset information
- details that would make an unpatched vulnerability easier to exploit

If the project repository has GitHub Security Advisories enabled, use the repository's **Report a vulnerability** option.

If private vulnerability reporting is not available, contact the maintainer privately using the contact details provided by the repository or project profile.

Please include, where possible:

- a clear description of the vulnerability
- the affected `dtabnk` version
- the operating system and Python version
- the input format or code path involved
- steps required to reproduce the issue
- the potential security impact
- any suggested mitigation or fix
- whether the issue has been disclosed elsewhere

## Response and Disclosure

Reports will be reviewed and assessed before public disclosure.

Where a vulnerability is confirmed, the preferred process is to:

1. reproduce and assess the issue
2. determine the affected versions and impact
3. prepare and test a fix
4. release the fix
5. publish appropriate security information after users have had a reasonable opportunity to update

Please allow time for investigation and remediation before publicly disclosing a confirmed vulnerability.

## Security Considerations

`dtabnk` processes external CSV and Excel files and can install missing Python dependencies when `pip` is available. Users should therefore apply normal precautions when processing untrusted data or running the programme in sensitive environments.

In particular:

- only process files from sources you trust where practical
- keep Python and project dependencies up to date
- use an isolated Python environment where appropriate
- review package installation prompts and dependency sources
- avoid running the programme with unnecessary elevated privileges
- do not store secrets, credentials, or confidential information in command-line arguments
- validate output files before using them in downstream analytical or production workflows

The programme's memory-safety checks are intended to reduce the risk of excessive memory use. They are not a security sandbox and do not make untrusted files inherently safe to process.

## Scope

Security reports may include issues involving:

- unsafe handling of malformed or malicious input files
- arbitrary code execution
- command or path injection
- unsafe temporary-file handling
- unintended file overwrite or deletion
- dependency-related security weaknesses
- disclosure of sensitive local information
- vulnerabilities in export or conversion paths that could compromise the host environment

General bugs, incorrect conversions, unsupported file layouts, performance problems, and feature requests should be reported through the normal issue tracker unless they have a security impact.

## Dependencies

`dtabnk` relies on third-party Python packages and, for some export formats, external runtime components.

Security issues originating in a dependency should also be reported to the relevant upstream project where appropriate. If a dependency vulnerability affects `dtabnk` directly, it may also be reported here so that compatibility constraints, mitigations, or dependency updates can be considered.

## Good-Faith Research

Good-faith security research intended to identify and responsibly report vulnerabilities is welcome.

Please avoid:

- accessing data that does not belong to you
- disrupting systems or services
- modifying or deleting third-party data
- using a vulnerability beyond what is necessary to demonstrate its existence
- publicly disclosing an unpatched issue before reasonable remediation efforts have been made

Thank you for helping to keep `dtabnk` and its users secure.
