import ast
import types
import pytest


def load_functions():
    with open('main.py', 'r', encoding='utf-8') as f:
        tree = ast.parse(f.read(), filename='main.py')
    func_names = {'normalize_code', 'is_gs1_datamatrix'}
    new_body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in func_names]
    mod = types.ModuleType('main_funcs')
    mod_ast = ast.Module(body=new_body, type_ignores=[])
    code = compile(mod_ast, 'main_funcs', 'exec')
    exec(code, mod.__dict__)
    # provide default implementation for dependency
    mod.get_settings_data = lambda: {}
    return mod

main = load_functions()


def test_normalize_code_no_separator():
    assert main.normalize_code('ABCDEF') == 'ABCDEF'


def test_normalize_code_with_separator():
    assert main.normalize_code('ABC\x1dDEF') == 'ABC'


def test_normalize_code_multiple_separators():
    assert main.normalize_code('123\x1d456\x1d789') == '123'


def test_normalize_code_separator_at_start():
    assert main.normalize_code('\x1dABC') == ''


def test_normalize_code_empty_string():
    assert main.normalize_code('') == ''


@pytest.mark.parametrize('length,expected', [(18, True), (17, False)])
def test_is_gs1_datamatrix_default(monkeypatch, length, expected):
    monkeypatch.setattr(main, 'get_settings_data', lambda: {})
    code = 'A' * length
    assert main.is_gs1_datamatrix(code) is expected


def test_is_gs1_datamatrix_custom_length(monkeypatch):
    monkeypatch.setattr(main, 'get_settings_data', lambda: {'len_gs1': 12})
    assert main.is_gs1_datamatrix('X' * 12)
    assert not main.is_gs1_datamatrix('X' * 13)
