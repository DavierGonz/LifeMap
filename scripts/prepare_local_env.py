"""Prepara secretos locales nuevos sin sobrescribirlos ni mostrar sus valores."""
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parents[1]


def secret_bytes(value):
    if len(value) < 24 or not value.isascii() or any(character.isspace() for character in value):
        raise ValueError('Expected a strong ASCII token without whitespace')
    # Escritura binaria para evitar CRLF de Windows en contrasenas SCRAM.
    return value.encode('ascii') + b'\n'


def main():
    directory = ROOT / '.secrets'
    if not directory.is_dir():
        raise SystemExit('Prepare the protected LifeMap .secrets directory first')
    paths = [directory / (name + '.txt') for name in ('mongo_root_password', 'mongo_writer_password', 'mongo_reader_password')]
    if any(path.exists() for path in [*paths, ROOT / '.env']):
        raise SystemExit('Refusing to overwrite existing local configuration')
    for path in paths:
        with path.open('xb') as output:
            output.write(secret_bytes(secrets.token_urlsafe(48)))
    with (ROOT / '.env').open('x', encoding='utf-8', newline='\n') as output:
        output.write((ROOT / '.env.example').read_text(encoding='utf-8'))
    print('Local files created; secret values are not displayed')


if __name__ == '__main__':
    main()
