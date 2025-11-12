# Dex Identity Provider - Local Testing Setup

This directory contains configuration for [Dex](https://dexidp.io/), an OpenID Connect (OIDC) identity provider used for JWT authentication testing.

## Overview

Dex is configured to provide:

- **OIDC Discovery**: `http://localhost:5556/dex/.well-known/openid-configuration`
- **JWKS Endpoint**: `http://localhost:5556/dex/keys`
- **Token Endpoint**: `http://localhost:5556/dex/token`
- **Authorization**: `http://localhost:5556/dex/auth`

## Quick Start

```bash
# Start Dex with docker-compose
cd /Users/sandrolain/work/events-bridge/localtest
docker-compose up -d dex

# Check Dex is running
curl http://localhost:5556/dex/.well-known/openid-configuration

# View logs
docker-compose logs -f dex
```

## Static Users

The configuration includes two test users:

| Email | Password | User ID |
|-------|----------|---------|
| <admin@example.com> | password | 08a8684b-db88-4b73-90a9-3cd1661f5466 |
| <user@example.com> | password | 41331323-6f44-45e6-b3b9-2c4b60c02be5 |

## Static Clients

### events-bridge-test

Primary client for integration tests:

- **Client ID**: `events-bridge-test`
- **Client Secret**: `events-bridge-secret`
- **Redirect URIs**:
  - `http://localhost:8080/callback`
  - `http://127.0.0.1:8080/callback`

### cli-tool

Public client for manual testing (no secret required):

- **Client ID**: `cli-tool`
- **Redirect URIs**:
  - `http://localhost:5555/callback`
  - `urn:ietf:wg:oauth:2.0:oob`

## Getting a Test Token

### Using cURL (Password Grant - for testing only)

```bash
# Get token using static password
curl -X POST http://localhost:5556/dex/token \
  -d grant_type=password \
  -d client_id=events-bridge-test \
  -d client_secret=events-bridge-secret \
  -d username=admin@example.com \
  -d password=password \
  -d scope="openid profile email"
```

### Using Authorization Code Flow

1. Open browser to: `http://localhost:5556/dex/auth?client_id=events-bridge-test&redirect_uri=http://localhost:8080/callback&response_type=code&scope=openid+profile+email`

2. Login with:
   - Email: `admin@example.com`
   - Password: `password`

3. Extract authorization code from redirect URL

4. Exchange code for tokens:

```bash
curl -X POST http://localhost:5556/dex/token \
  -d grant_type=authorization_code \
  -d code=YOUR_AUTH_CODE \
  -d redirect_uri=http://localhost:8080/callback \
  -d client_id=events-bridge-test \
  -d client_secret=events-bridge-secret
```

## Events Bridge Configuration

Example configuration to validate JWT tokens from Dex:

```yaml
runners:
  - name: jwt-validator
    type: jwt
    config:
      enabled: true
      tokenMetadataKey: "authorization"
      tokenPrefix: "Bearer "
      jwksUrl: "http://localhost:5556/dex/keys"
      jwksRefreshInterval: "1h"
      issuer: "http://localhost:5556/dex"
      audience: ""  # Dex doesn't set audience by default
      requiredClaims:
        - "sub"
        - "email"
      claimPrefix: "jwt_"
      failOnError: true
      allowedAlgorithms:
        - "RS256"
      clockSkew: "60s"
```

## Integration Tests

Run integration tests with Dex:

```bash
# Run integration tests
go test -tags=integration ./src/connectors/jwt/...

# Run with verbose output
go test -tags=integration -v ./src/connectors/jwt/...
```

## Token Claims

ID tokens from Dex include these standard claims:

- `iss`: Issuer (`http://localhost:5556/dex`)
- `sub`: Subject (user ID)
- `aud`: Audience (client ID)
- `exp`: Expiration time
- `iat`: Issued at time
- `email`: User email
- `email_verified`: Email verification status
- `name`: User's display name

## Useful Endpoints

| Endpoint | URL | Description |
|----------|-----|-------------|
| Discovery | <http://localhost:5556/dex/.well-known/openid-configuration> | OIDC discovery document |
| JWKS | <http://localhost:5556/dex/keys> | JSON Web Key Set |
| Token | <http://localhost:5556/dex/token> | Token endpoint |
| Auth | <http://localhost:5556/dex/auth> | Authorization endpoint |
| Userinfo | <http://localhost:5556/dex/userinfo> | User info endpoint |
| Telemetry | <http://localhost:5558/metrics> | Prometheus metrics |

## Troubleshooting

### Dex not starting

```bash
# Check logs
docker-compose logs dex

# Verify config syntax
docker-compose config

# Restart service
docker-compose restart dex
```

### Token validation failing

1. Check issuer matches: `http://localhost:5556/dex`
2. Verify JWKS URL is accessible
3. Ensure token hasn't expired
4. Check algorithm is RS256
5. Verify client_id and client_secret

### Database locked errors

```bash
# Remove database and restart
docker-compose down -v
docker-compose up -d dex
```

## Advanced Configuration

The Dex configuration supports:

- **Multiple connectors**: LDAP, GitHub, Google, etc.
- **Custom themes**: Branding and styling
- **gRPC API**: Management operations
- **Token expiry**: Configurable lifetimes
- **Refresh tokens**: Long-lived sessions

See [Dex documentation](https://dexidp.io/docs/) for more options.

## Security Notes

⚠️ **This configuration is for local testing only!**

- Uses HTTP instead of HTTPS
- Static passwords in plain text (bcrypt hashed)
- SQLite storage (not production-ready)
- Skip approval screen enabled
- Mock connectors for easy testing

For production use, configure:

- TLS/HTTPS with valid certificates
- External authentication (LDAP, OIDC, SAML)
- Persistent storage (PostgreSQL, MySQL)
- Proper secret management
- Rate limiting and security policies

## References

- [Dex Official Documentation](https://dexidp.io/docs/)
- [Dex Docker Hub](https://hub.docker.com/r/dexidp/dex)
- [OpenID Connect Specification](https://openid.net/specs/openid-connect-core-1_0.html)
- [RFC 7519 - JSON Web Token (JWT)](https://tools.ietf.org/html/rfc7519)
