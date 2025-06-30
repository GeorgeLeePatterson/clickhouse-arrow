# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.x.x   | :white_check_mark: |

## Reporting a Vulnerability

We take the security of clickhouse-arrow seriously. If you have discovered a security vulnerability, please follow these steps:

### 1. Do NOT disclose publicly

Please do not create a public GitHub issue for security vulnerabilities.

### 2. Email us directly

Send details to: [patterson.george@gmail.com]

Include:
- Type of vulnerability
- Affected components
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### 3. Response time

- We will acknowledge receipt within 48 hours
- We will provide a detailed response within 7 days
- We will work on a fix and coordinate disclosure

## Security Best Practices

When using clickhouse-arrow:

### Authentication
- Always use strong passwords
- Use TLS connections in production
- Rotate credentials regularly
- Never commit credentials to version control

### Network Security
- Use TLS for connections over untrusted networks
- Restrict ClickHouse server access to trusted IPs
- Use firewall rules to limit exposure

### Data Validation
- Validate all user input before queries
- Use parameterized queries to prevent SQL injection
- Be cautious with dynamic query construction

### Dependencies
- Keep clickhouse-arrow updated
- Monitor security advisories for dependencies
- Run `cargo audit` regularly

## Known Security Considerations

### Connection Security
- Always use TLS in production environments

### Query Construction
- The library does not automatically escape user input
- Applications must validate and sanitize input

### Memory Safety
- Built with Rust's memory safety guarantees
- No known memory safety issues

## Acknowledgments

We appreciate responsible disclosure and will acknowledge security researchers who help improve clickhouse-arrow.
