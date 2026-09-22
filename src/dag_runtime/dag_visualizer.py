"""Optional DOT export and Graphviz executable rendering, separate from the engine."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess

from .dag_model import Certainty, DAG, EdgeKind, EffectKind


def _quote(text: str) -> str:
    # DOT quoted strings accept JSON's common escaping; keep unicode readable.
    return json.dumps(text, ensure_ascii=False)


def to_dot(dag: DAG, *, mode: str = 'simple', edge_labels: bool = True) -> str:
    if mode not in {'simple', 'detailed'}:
        raise ValueError("mode must be 'simple' or 'detailed'")
    lines = ['digraph DAG {', '  rankdir=TB;', '  graph [bgcolor="white", pad="0.25"];',
             '  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11];',
             '  edge [fontname="Helvetica", fontsize=9, color="#64748b"];']
    declared_outputs = {event.value_id for event in dag.bindings}
    for task in dag.tasks.values():
        conservative = task.certainty == Certainty.CONSERVATIVE
        outputs = [dag.values[v].label for v in task.outputs if not dag.values[v].name.startswith('@')
                   and (mode == 'detailed' or v in declared_outputs or dag.values[v].origin == 'result')]
        if mode == 'simple' and len(outputs) > 4:
            outputs = outputs[:4] + [f'… +{len(outputs)-4} values']
        label = f'{task.id}\n{task.label}'
        if outputs:
            label += '\n' + ', '.join(outputs)
        if mode == 'detailed':
            label += f'\nline {task.span.line} · {task.certainty.value}'
            label += '\n' + task.placement
            label += f'\n{task.effect.value} · namespace epoch {task.namespace_epoch}'
            if conservative:
                label += '\n' + '\n'.join(task.conservative_reasons)
        color = '#ffedd5' if conservative else '#dbeafe'
        if task.effect == EffectKind.OBJECT_LOCAL:
            color = '#dcfce7'
        if not task.runnable:
            color = '#fecaca'
        lines.append(f'  {_quote(task.id)} [label={_quote(label)}, fillcolor="{color}"];')
    for edge in dag.edges.values():
        conservative = any(r.certainty == Certainty.CONSERVATIVE for r in edge.reasons)
        attrs = ['style="dashed"', 'color="#c2410c"'] if conservative else []
        if edge_labels:
            if mode == 'detailed':
                labels = [r.text for r in edge.reasons]
            else:
                labels = [dag.values[r.value_id].label for r in edge.reasons
                          if r.value_id is not None and r.kind == EdgeKind.DATA]
                if any(r.kind == EdgeKind.STATE and r.certainty == Certainty.CERTAIN for r in edge.reasons):
                    labels.append('object state')
                if conservative:
                    labels.append('conservative order/state')
            attrs.append('label=' + _quote('\n'.join(dict.fromkeys(labels))))
        lines.append(f'  {_quote(edge.source)} -> {_quote(edge.target)} [{", ".join(attrs)}];')
    lines.append('}')
    return '\n'.join(lines) + '\n'


def write_dot(dag: DAG, path: str | Path, *, mode: str = 'simple', edge_labels: bool = True) -> Path:
    path = Path(path)
    path.write_text(to_dot(dag, mode=mode, edge_labels=edge_labels), encoding='utf-8')
    return path


class GraphvizUnavailable(RuntimeError):
    pass


def render_dot(dot_path: str | Path, output_path: str | Path, *, format: str | None = None,
               timeout: float = 30) -> Path:
    """Invoke `dot` without a shell. The DOT file remains if rendering fails."""
    dot_path, output_path = Path(dot_path), Path(output_path)
    format = format or output_path.suffix.lstrip('.').lower()
    if format not in {'svg', 'png'}:
        raise ValueError("render format must be 'svg' or 'png'")
    executable = shutil.which('dot')
    if executable is None:
        raise GraphvizUnavailable(f'Graphviz dot is not installed; DOT is available at {dot_path}')
    subprocess.run([executable, '-T'+format, str(dot_path.resolve()), '-o', str(output_path.resolve())],
                   check=True, capture_output=True, text=True, timeout=timeout)
    return output_path


def main(argv=None) -> int:
    from .dag_engine import analyze_file
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    parser.add_argument('--dot', type=Path, required=True)
    parser.add_argument('--render', type=Path, help='optional output .svg or .png')
    parser.add_argument('--mode', choices=['simple', 'detailed'], default='simple')
    parser.add_argument('--no-edge-labels', action='store_true')
    args = parser.parse_args(argv)
    dag = analyze_file(args.file)
    write_dot(dag, args.dot, mode=args.mode, edge_labels=not args.no_edge_labels)
    print(f'DOT written: {args.dot}')
    if args.render:
        try:
            render_dot(args.dot, args.render)
        except GraphvizUnavailable as error:
            print(error)
        else:
            print(f'Rendered: {args.render}')
    return 0 if dag.execution_permitted else 2


if __name__ == '__main__':
    raise SystemExit(main())
