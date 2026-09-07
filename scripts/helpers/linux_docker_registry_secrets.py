"""Ephemeral private inputs for the source-selected Distribution fixture.

No filesystem, subprocess, environment, network, or Docker access. Explicit
private accessors return sensitive bytes for an owned stdin transport; callers
must register canaries before dispatch. Python does not guarantee memory erasure.
Public certificate metadata is not evidence of a live TLS handshake.

Distribution configuration contract:
https://github.com/distribution/distribution/blob/v3.1.1/configuration/configuration.go
https://github.com/distribution/distribution/blob/v3.1.1/registry/auth/htpasswd/access.go
"""
import copy
import datetime
import importlib.metadata
import io
import ipaddress
from pathlib import Path
import secrets
import tarfile

import bcrypt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from cryptography.x509.verification import PolicyBuilder, Store, VerificationError

import linux_docker_registry_fixture as fixture

USERNAME = 'vz-registry-user'
FILES = ('ca.crt', 'server.crt', 'server.key', 'htpasswd', 'config.yml')
LIMIT = 64 * 1024
DEPENDENCIES = {'cryptography': '50.0.1', 'bcrypt': '5.0.0'}
UTC = datetime.timezone.utc


class SecretsError(ValueError):
    """Fixed diagnostics never interpolate private inputs."""


def require(condition, code):
    if not condition:
        raise SecretsError('registry secrets: ' + code)


def dependency_inputs():
    """Version admission only; the runner separately authenticates its lock."""
    versions = {name: importlib.metadata.version(name) for name in DEPENDENCIES}
    require(versions == DEPENDENCIES, 'dependency version mismatch')
    return versions


def _pem(cert):
    return cert.public_bytes(serialization.Encoding.PEM)


def _spki(key):
    return key.public_bytes(serialization.Encoding.DER,
                            serialization.PublicFormat.SubjectPublicKeyInfo)


def _time(ns):
    require(type(ns) is int and 60 * 10**9 < ns < 2**63 - 8 * 86400 * 10**9,
            'observation time bounds')
    return datetime.datetime.fromtimestamp(ns // 10**9, UTC)


def _certificate(key, name, start, end, *, issuer=None, issuer_key=None, address=None):
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    ca = issuer is None
    builder = (x509.CertificateBuilder().subject_name(subject)
        .issuer_name(subject if ca else issuer.subject)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(start).not_valid_after(end)
        .add_extension(x509.BasicConstraints(ca=ca, path_length=0 if ca else None), critical=True)
        .add_extension(x509.KeyUsage(digital_signature=not ca, content_commitment=False,
            key_encipherment=False, data_encipherment=False, key_agreement=False,
            key_cert_sign=ca, crl_sign=ca, encipher_only=False, decipher_only=False), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(
            key.public_key() if ca else issuer_key.public_key()), critical=False))
    if not ca:
        builder = (builder.add_extension(x509.SubjectAlternativeName([
            x509.IPAddress(ipaddress.ip_address(address))]), critical=False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False))
    return builder.sign(key if ca else issuer_key, hashes.SHA256())


def _config(spec, http_secret):
    # JSON is a YAML subset. No proxy, hooks, debug listener, environment secret,
    # or access log is configured. The htpasswd file must exist before startup.
    root = spec['container_fixture_directory']
    return fixture.canonical({'version': '0.1',
        'log': {'level': 'info', 'formatter': 'json', 'accesslog': {'disabled': True}},
        'storage': {'filesystem': {'rootdirectory': '/var/lib/registry'}},
        'auth': {'htpasswd': {'realm': 'vz-private-registry', 'path': root + '/htpasswd'}},
        # Distribution installs SIGTERM handling only for nonzero DrainTimeout.
        # Five seconds fits the caller's ten-second Docker stop deadline.
        'http': {'addr': spec['authority'], 'secret': http_secret.decode('ascii'), 'draintimeout': '5s',
                 'tls': {'certificate': root + '/server.crt', 'key': root + '/server.key',
                         'minimumtls': 'tls1.2'}}})


class Secrets:
    """One fresh per-Machine fixture; private data has no implicit serializer."""

    __slots__ = ('_spec', '_now', '_valid', '_invalid', '_http', '_hash', '_key_pem',
                 '_ca', '_server', '_wrong', '_files')

    def __repr__(self):
        return '<Secrets: private in-memory registry fixture>'

    def __reduce__(self):
        raise SecretsError('registry secrets: serialization forbidden')

    @classmethod
    def generate(cls, owner, run_id, *, now_unix_ns):
        dependency_inputs()
        spec = fixture.resource_spec(owner, run_id)
        now = _time(now_unix_ns)
        start, end = now - datetime.timedelta(seconds=60), now + datetime.timedelta(days=7)
        result = cls()
        result._spec, result._now = copy.deepcopy(spec), now_unix_ns
        result._valid = secrets.token_hex(24).encode('ascii')
        result._invalid = secrets.token_hex(24).encode('ascii')
        result._http = secrets.token_hex(32).encode('ascii')
        require(len({result._valid, result._invalid, result._http}) == 3, 'entropy collision')
        ca_key, server_key, wrong_key = (ec.generate_private_key(ec.SECP256R1()) for _ in range(3))
        result._ca = _certificate(ca_key, 'vz registry fixture CA', start, end)
        result._wrong = _certificate(wrong_key, 'vz registry wrong CA', start, end)
        result._server = _certificate(server_key, 'vz registry fixture', start, end,
            issuer=result._ca, issuer_key=ca_key, address=spec['address'])
        result._key_pem = server_key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        result._hash = bcrypt.hashpw(result._valid, bcrypt.gensalt(rounds=12, prefix=b'2b'))
        result._files = {'ca.crt': _pem(result._ca), 'server.crt': _pem(result._server),
            'server.key': result._key_pem, 'htpasswd': USERNAME.encode() + b':' + result._hash + b'\n',
            'config.yml': _config(spec, result._http)}
        result.validate_private()
        return result

    @property
    def pins(self):
        """Public certificate PEM and public-key DER fingerprints only."""
        return {'ca_sha256': fixture.sha(self.ca_pem()),
                'certificate_sha256': fixture.sha(self._files['server.crt']),
                'spki_sha256': fixture.sha(_spki(self._server.public_key()))}

    def ca_pem(self, *, wrong=False):
        require(type(wrong) is bool, 'CA selector')
        return _pem(self._wrong if wrong else self._ca)

    def password(self, role='valid'):
        require(role in ('valid', 'invalid'), 'password role')
        return self._valid if role == 'valid' else self._invalid

    def canaries(self):
        """Private values for in-memory leak scanners, never public evidence."""
        body_lines = tuple(line for line in self._key_pem.splitlines() if not line.startswith(b'-----'))
        return (self._valid, self._invalid, self._http, self._hash,
                self._key_pem, b''.join(body_lines), *body_lines)

    def privatefiles(self):
        return dict(self._files)

    def public(self):
        cert = self._server
        return {'schema_version': 1, 'owner': copy.deepcopy(self._spec['owner']),
            'run_id': self._spec['run_id'], 'authority': self._spec['authority'], **self.pins,
            'issuer_ca_sha256': self.pins['ca_sha256'], 'san_ips': [self._spec['address']],
            'san_dns': [], 'is_ca': False, 'ca_is_ca': True,
            'key_usage': ['digital_signature'], 'extended_key_usage': ['server_auth'],
            'not_before_unix_ns': int(cert.not_valid_before_utc.timestamp()) * 10**9,
            'not_after_unix_ns': int(cert.not_valid_after_utc.timestamp()) * 10**9}

    def validate_private(self, *, observed_unix_ns=None):
        """Native chain, exact identity, key, and password checks; no handshake."""
        try:
            now_ns = self._now if observed_unix_ns is None else observed_unix_ns
            now = _time(now_ns)
            require(set(self._files) == set(FILES) and all(type(v) is bytes and v for v in self._files.values())
                    and sum(map(len, self._files.values())) < LIMIT // 2, 'private file bounds')
            require(self._files['ca.crt'] == _pem(self._ca) and
                    self._files['server.crt'] == _pem(self._server) and
                    self._files['server.key'] == self._key_pem, 'certificate material changed')
            key = serialization.load_pem_private_key(self._key_pem, password=None)
            require(isinstance(key, ec.EllipticCurvePrivateKey) and
                    isinstance(key.curve, ec.SECP256R1) and
                    _spki(key.public_key()) == _spki(self._server.public_key()), 'private key mismatch')
            san = self._server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
            require(list(san) == [x509.IPAddress(ipaddress.ip_address(self._spec['address']))], 'exact IP SAN')
            self._ca.verify_directly_issued_by(self._ca)
            verifier = PolicyBuilder().store(Store([self._ca])).time(now).build_server_verifier(
                x509.IPAddress(ipaddress.ip_address(self._spec['address'])))
            require(verifier.verify(self._server, []) == [self._server, self._ca], 'certificate chain')
            wrong = PolicyBuilder().store(Store([self._wrong])).time(now).build_server_verifier(
                x509.IPAddress(ipaddress.ip_address(self._spec['address'])))
            try:
                wrong.verify(self._server, [])
            except VerificationError:
                pass
            else:
                raise SecretsError('registry secrets: wrong CA accepted')
            require(len(self._valid) == len(self._invalid) == 48 and self._valid != self._invalid and
                    self._hash.startswith(b'$2b$12$') and bcrypt.checkpw(self._valid, self._hash) and
                    not bcrypt.checkpw(self._invalid, self._hash), 'password validation')
            require(self._files['htpasswd'] == USERNAME.encode() + b':' + self._hash + b'\n' and
                    self._files['config.yml'] == _config(self._spec, self._http), 'private configuration changed')
            fixture.validate_tls_public(self.public(), spec=self._spec, expected=self.pins,
                                        observed_unix_ns=now_ns)
            return {'certificate_chain_verified': True, 'wrong_ca_rejected': True,
                    'password_positive_and_negative_verified': True, 'handshake_certified': False,
                    'registry_authentication_certified': False}
        except Exception:
            # Native exceptions can contain supplied bytes or arbitrary fields.
            raise SecretsError('registry secrets: private validation failed') from None

    def provision_tar(self):
        self.validate_private()
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode='w', format=tarfile.USTAR_FORMAT) as archive:
            for name in sorted(FILES):
                raw = self._files[name]
                member = tarfile.TarInfo(name)
                member.size, member.mode = len(raw), 0o600
                member.uid = member.gid = member.mtime = 0
                archive.addfile(member, io.BytesIO(raw))
        result = output.getvalue()
        require(len(result) < LIMIT, 'private archive bounds')
        return result


def required_source_paths():
    return sorted({str(Path(__file__).resolve()), *fixture.required_source_paths()})
