"""Reproducible analyzer measurements, never execution/profiling of input programs.

Run from the project root. --engine-dir also accepts an unchanged v1 checkout.
Each sample collects prior analyzer garbage outside the timed interval, then
measures analyze_source (including graph validation). No runtime tasks run.
"""
from __future__ import annotations

import argparse
import ast
import gc
import json
from pathlib import Path
import platform
import statistics
import sys
import time


CASES = {
    'container_reads': 'a=(10,20)\nb=(30,40)\nx=a[1]\ny=b[0]\nresult=x+y\n',
    'comprehension_branches': '''def clean(values): return [x*2 for x in values]
def adjust(values): return [x+1 for x in values]
a=[1,2]
b=[3,4]
x=clean(a)
y=adjust(b)
result=sum(x)+sum(y)
''',
    'object_local': 'a=[1,2]\nalias=a\nb=[4,5]\nx=sum(a)\nu=sum(b)\na.append(3)\ny=sum(alias)\nv=sum(b)\n',
    'reflection_recovery': 'a=1\nb=2\ngetattr(obj,name)\n1+2\n3*4\n',
    'exception_recovery': 'def f(x): return x+1\nn=2\nq=10//n\na=f(2)\nb=f(3)\nresult=a+b\n',
    'ordinary_import': 'import math\n1+2\n3*4\n',
    'import_named_guard': 'import math\ndef f(): return 1\na=f()\nb=f()\n',
    'unknown_named_guard': 'unknown()\na=1\nb=2\n',
    'namespace_escape': 'a=1\ng=globals()\nx=2\ny=3\n',
}


def metrics(dag):
    """Same definitions for both versions; do not infer effects v1 never exported."""
    levels, widths = {}, {}
    for ident in dag.topological_order():
        level = max((levels[p]+1 for p in dag.tasks[ident].dependencies), default=0)
        levels[ident]=level
        widths[level]=widths.get(level,0)+1
    tails = [t for t in dag.tasks.values() if getattr(t,'region_scope',None)=='tail' or
             t.ast_type=='Module' and t.kind=='opaque_region']
    hidden=0
    for tail in tails:
        # V1's synthetic Module span incorrectly started at line 1. Count the
        # intended suffix from prior task/binding spans rather than crediting a
        # span bug as a precision improvement. V2 exports statement_count.
        if hasattr(tail,'statement_count'):
            count=tail.statement_count
        else:
            prefix_end=max([t.span.end_line for t in dag.tasks.values() if t.id!=tail.id] +
                           [e.span.end_line for e in dag.bindings if e.kind!='region'] + [0])
            count=sum(n.lineno>prefix_end for n in ast.parse(dag.source).body)
        hidden+=max(0,count-1)
    return {
        'tasks':len(dag.tasks), 'values':len(dag.values), 'edge_pairs':len(dag.edges),
        'certain_tasks':sum(t.certainty.value=='certain' for t in dag.tasks.values()),
        'conservative_tasks':sum(t.certainty.value=='conservative' for t in dag.tasks.values()),
        'edges_by_kind':{kind:sum(any(r.kind.value==kind for r in e.reasons) for e in dag.edges.values())
                         for kind in ('data','state','order')},
        'opaque_regions':sum(t.kind in {'opaque_region','opaque_module'} for t in dag.tasks.values()),
        'whole_tail_collapses':len(tails),
        'hidden_statements':hidden,
        'isolated_candidates':sum(t.placement=='isolated_candidate' for t in dag.tasks.values()),
        'max_generation_width':max(widths.values(),default=0),
    }


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine-dir',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--graphs-dir',type=Path)
    parser.add_argument('--repeats',type=int,default=5)
    parser.add_argument('--sizes',type=int,nargs='+',default=[1000,5000,10000])
    args=parser.parse_args(argv)
    if args.repeats<1 or any(n<1 for n in args.sizes):
        parser.error('repeats and sizes must be positive')
    sys.path.insert(0,str(args.engine_dir.resolve().parent))
    from dag_runtime.dag_engine import analyze_source
    from dag_runtime.dag_visualizer import to_dot

    result={'python':platform.python_version(),'platform':platform.platform(),
            'repeats':args.repeats,'method':'one warmup; gc.collect before each timed analyze_source; median',
            'benchmarks':{},'examples':{}}
    workloads={f'chain_{n}':'x=0\n'+'x=x+1\n'*(n-1) for n in args.sizes}
    workloads['comprehensions_500']='def clean(v): return [x*2 for x in v]\n'+''.join(
        f'a{i}=[1,2,3]\nb{i}=clean(a{i})\nc{i}=sum(b{i})\n' for i in range(500))
    workloads['objects_500']=''.join(f'a{i}=[1,2]\nx{i}=sum(a{i})\na{i}.append(3)\ny{i}=sum(a{i})\n'
                                    for i in range(500))
    for name,source in workloads.items():
        warm=analyze_source(source)
        del warm
        samples=[]
        for _ in range(args.repeats):
            gc.collect()
            start=time.perf_counter()
            dag=analyze_source(source)
            samples.append(time.perf_counter()-start)
            counts={'tasks':len(dag.tasks),'values':len(dag.values),'edge_pairs':len(dag.edges)}
            del dag
        result['benchmarks'][name]={'median_seconds':statistics.median(samples),'samples_seconds':samples,**counts}
    if args.graphs_dir:
        args.graphs_dir.mkdir(parents=True,exist_ok=True)
    for name,source in CASES.items():
        dag=analyze_source(source,filename=name+'.py')
        result['examples'][name]={'source':source,'metrics':metrics(dag),
                                 'tasks':[{'id':t.id,'source':t.source,'certainty':t.certainty.value,
                                           'dependencies':sorted(t.dependencies),'placement':t.placement}
                                          for t in dag.tasks.values()]}
        if args.graphs_dir:
            (args.graphs_dir/(name+'.dot')).write_text(to_dot(dag),encoding='utf-8')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({name:round(v['median_seconds'],6) for name,v in result['benchmarks'].items()},indent=2))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
