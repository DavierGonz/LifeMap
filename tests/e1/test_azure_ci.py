import pytest
from ci.azure_promote import replace_upstream,HOST

def test_switch_preserves_other_directives():
    source=f'{HOST} {{\n encode gzip\n reverse_proxy 127.0.0.1:5100\n}}\n'
    changed=replace_upstream(source,5101)
    assert changed.replace(':5101',':5100')==source
    assert replace_upstream(changed,5100)==source

@pytest.mark.parametrize('text,port',[('wrong.example { reverse_proxy 127.0.0.1:5100 }',5101),(HOST,5101),(HOST+' reverse_proxy 127.0.0.1:5100',27017)])
def test_rejects_unexpected_configuration(text,port):
    with pytest.raises(ValueError):replace_upstream(text,port)
