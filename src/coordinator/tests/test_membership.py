import protocol as p

from coordinator.tests.helpers import connect


def test_membership_update_sent_to_existing_worker_on_join(coordinator, build_plan):
    _, plan=build_plan("a=1\n")
    w1=connect(coordinator,plan,"W1",port=9001)
    connect(coordinator,plan,"W2",port=9002)
    updates=[m for m in w1.drain() if isinstance(m,p.MembershipUpdate)]
    assert len(updates)==1
    assert {e.worker_id for e in updates[0].members}=={"W1","W2"}


def test_membership_update_removes_departed_worker(coordinator, build_plan):
    _, plan=build_plan("a=1\n")
    w1=connect(coordinator,plan,"W1",port=9001)
    w2=connect(coordinator,plan,"W2",port=9002)
    w1.drain()  # join update
    w2.send(p.WorkerGoodbye("W2","bye",message_id="bye"))
    updates=[m for m in w1.drain() if isinstance(m,p.MembershipUpdate)]
    assert updates
    assert {e.worker_id for e in updates[-1].members}=={"W1"}
