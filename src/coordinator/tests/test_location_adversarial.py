import pytest
import protocol as p

from coordinator import EventDisposition, InvalidDataLocation, StaleWorkerSession, UnknownRun
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed
from scheduler import DataForm


def immutable_ref(plan, run_id="r"):
    value=next(v for v in plan.values if v.storage=="immutable_value")
    return p.DataReference(plan.id,run_id,value.id,DataForm.IMMUTABLE_VALUE)


def test_duplicate_object_available_is_idempotent(coordinator,build_plan):
    _,plan=build_plan("a=1\nb=a+1\n")
    w=connect(coordinator,plan); coordinator.submit(plan,run_id="r")
    coordinator.schedule("r")
    d=dispatch_for(w); accept(w,d); start(w,d); succeed(w,plan,d)
    data=immutable_ref(plan)
    msg=p.ObjectAvailable("W1",data,10,message_id="a")
    assert w.send(msg)==EventDisposition.DUPLICATE
    assert w.send(msg)==EventDisposition.DUPLICATE
    location,=coordinator.data_locations("r")
    assert len(location.replicas)==1


def test_conflicting_size_for_same_representation_rejected(coordinator,build_plan):
    _,plan=build_plan("a=1\nb=a+1\n")
    w=connect(coordinator,plan); coordinator.submit(plan,run_id="r")
    coordinator.schedule("r")
    d=dispatch_for(w); accept(w,d); start(w,d); succeed(w,plan,d)
    data=immutable_ref(plan)
    w.send(p.ObjectAvailable("W1",data,10,message_id="a"))
    with pytest.raises(InvalidDataLocation):
        w.send(p.ObjectAvailable("W1",data,11,message_id="b"))
    assert coordinator.data_locations("r")[0].size_bytes==10


def test_foreign_run_data_rejected(coordinator,build_plan):
    _,plan=build_plan("a=1\n")
    w=connect(coordinator,plan); coordinator.submit(plan,run_id="r")
    value=next(v for v in plan.values if v.storage=="immutable_value")
    foreign=p.DataReference(plan.id,"other",value.id,DataForm.IMMUTABLE_VALUE)
    with pytest.raises(UnknownRun):
        w.send(p.ObjectAvailable("W1",foreign,10,message_id="foreign"))
    assert coordinator.data_locations("r")==()


def test_stale_session_cannot_restore_evicted_replica(coordinator,build_plan):
    _,plan=build_plan("a=1\nb=a+1\n")
    old=connect(coordinator,plan,"W1",port=9001); coordinator.submit(plan,run_id="r")
    coordinator.schedule("r")
    d=dispatch_for(old); accept(old,d); start(old,d); succeed(old,plan,d)
    data=immutable_ref(plan)
    old.send(p.ObjectAvailable("W1",data,10,message_id="old-available"))
    new=connect(coordinator,plan,"W1",port=9002)
    assert coordinator.data_locations("r")==()
    with pytest.raises(StaleWorkerSession):
        old.send(p.ObjectAvailable("W1",data,10,message_id="late-old"))
    assert coordinator.data_locations("r")==()
    assert new.handle.generation>old.handle.generation


def test_nontransferable_state_token_cannot_be_announced_as_data(coordinator,build_plan):
    _,plan=build_plan("a=[1]\na.append(2)\n")
    w=connect(coordinator,plan); coordinator.submit(plan,run_id="r")
    state=next(v for v in plan.values if v.origin=="object_state")
    forged=p.DataReference(plan.id,"r",state.id,DataForm.IMMUTABLE_VALUE)
    with pytest.raises(InvalidDataLocation):
        w.send(p.ObjectAvailable("W1",forged,1,message_id="bad-state"))
