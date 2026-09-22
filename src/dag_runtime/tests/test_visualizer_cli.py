"""DOT export is independent of Graphviz; rendering is an optional executable adapter."""
from pathlib import Path
import subprocess
import sys

import pytest

from dag_runtime.dag_engine import analyze_file
from dag_runtime import dag_visualizer as viz


def test_dot_modes_and_conservative_style(analyze):
    """Simple DOT shows IDs/outputs and dashed barriers; detailed DOT includes reasons."""
    dag=analyze('a=1\nunknown(a)')
    simple=viz.to_dot(dag)
    detailed=viz.to_dot(dag,mode='detailed')
    assert 'digraph DAG' in simple and 'a#1' in simple
    assert 'style="dashed"' in simple
    assert 'conservative barrier' in detailed and len(detailed)>len(simple)
    assert 'conservative order/state' not in viz.to_dot(dag,edge_labels=False)
    with pytest.raises(ValueError): viz.to_dot(dag,mode='typo')


def test_dot_escaping(analyze):
    """Quotes, newlines, and backslashes are escaped in labels, preventing malformed DOT."""
    dag=analyze('getattr(obj,"quoted\\\"name")()')
    dot=viz.to_dot(dag,mode='detailed')
    assert '\\n' in dot and dot.endswith('}\n')
    assert viz._quote('a"b\\c\nd')=='"a\\"b\\\\c\\nd"'


def test_missing_graphviz_preserves_dot(analyze,tmp_path,monkeypatch):
    """Absent dot executable raises a specific rendering error but leaves usable DOT intact."""
    dag=analyze('a=1')
    path=viz.write_dot(dag,tmp_path/'graph.dot')
    monkeypatch.setattr(viz.shutil,'which',lambda _:None)
    with pytest.raises(viz.GraphvizUnavailable,match='not installed'):
        viz.render_dot(path,tmp_path/'graph.svg')
    assert path.exists() and path.read_text().startswith('digraph')


@pytest.mark.parametrize('format',['svg','png'])
def test_renderer_arguments_no_shell(tmp_path,monkeypatch,format):
    """Rendering passes explicit dot arguments, a checked return code, and a finite timeout."""
    calls=[]
    monkeypatch.setattr(viz.shutil,'which',lambda _:'/fake/dot')
    monkeypatch.setattr(viz.subprocess,'run',lambda args,**kwargs:calls.append((args,kwargs)))
    dot=tmp_path/'has spaces.dot'
    dot.write_text('digraph {}')
    out=tmp_path/('result.'+format)
    assert viz.render_dot(dot,out)==out
    args,options=calls[0]
    assert args==['/fake/dot','-T'+format,str(dot.resolve()),'-o',str(out.resolve())]
    assert options['check'] is True and options['timeout']==30
    assert not options.get('shell',False)


def test_invalid_render_format(tmp_path):
    """The renderer rejects non-SVG/PNG output formats before invoking external programs."""
    with pytest.raises(ValueError,match='format'):
        viz.render_dot(tmp_path/'a.dot',tmp_path/'a.exe')


def test_analyze_file_encoding_cookie(tmp_path):
    """analyze_file honors Python source encodings and extracts UTF-8 AST columns correctly."""
    path=tmp_path/'latin.py'
    path.write_bytes('# coding: latin-1\ncafé=1\ny=café+2\n'.encode('latin-1'))
    dag=analyze_file(path)
    a,b=list(dag.tasks.values())
    assert a.source=='café=1' and b.source=='y=café+2'
    assert b.dependencies=={a.id}


def test_engine_cli_exports_json(tmp_path):
    """Engine CLI emits JSON with values/reasons and exits successfully for valid source."""
    root=Path(__file__).resolve().parents[1]
    source=tmp_path/'source.py'
    source.write_text('a=1\nb=a+1\n')
    output=tmp_path/'graph.json'
    result=subprocess.run([sys.executable,'-m','dag_runtime.dag_engine',str(source),'--json',str(output)],cwd=root.parent,capture_output=True,text=True)
    assert result.returncode==0 and output.exists()
    assert 'initially ready:' in result.stdout
    import json
    data=json.loads(output.read_text())
    assert len(data['tasks'])==2 and len(data['edges'])==1


def test_visualizer_cli_dot_without_renderer(tmp_path):
    """The public visualizer CLI generates DOT without requiring Graphviz or its Python package."""
    root=Path(__file__).resolve().parents[1]
    source=tmp_path/'source.py'
    source.write_text('a=1\nb=a+1\n')
    output=tmp_path/'graph.dot'
    result=subprocess.run([sys.executable,'-m','dag_runtime.dag_visualizer',str(source),'--dot',str(output)],cwd=root.parent,capture_output=True,text=True)
    assert result.returncode==0 and output.read_text().startswith('digraph DAG')


def test_invalid_source_cli_nonzero_with_diagnostic_dot(tmp_path):
    """Malformed Python yields a diagnostic DOT file while the CLI returns exit status 2."""
    root=Path(__file__).resolve().parents[1]
    source=tmp_path/'bad.py'
    source.write_text('return 1')
    output=tmp_path/'bad.dot'
    result=subprocess.run([sys.executable,'-m','dag_runtime.dag_visualizer',str(source),'--dot',str(output)],cwd=root.parent,capture_output=True,text=True)
    assert result.returncode==2 and 'invalid Python source' in output.read_text()
