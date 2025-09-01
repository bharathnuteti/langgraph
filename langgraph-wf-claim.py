from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import uuid

from langgraph.graph import StateGraph, END

# ==========
# Utilities
# ==========
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm(s: Optional[str]) -> Optional[str]:
    return s.lower().strip() if isinstance(s, str) else s

# ==========
# State model
# ==========
@dataclass
class WorkflowState:
    # Identity
    instance_id: str
    customer_id: str
    workflow_name: str

    # Actors
    started_by: str
    last_actor: Optional[str] = None

    # Lifecycle
    status: str = "not_started"  # not_started, in_progress, paused, hold, completed, aborted, failed
    last_node: Optional[str] = None

    # Timestamps
    start_time: Optional[str] = None
    last_pause_time: Optional[str] = None
    last_resume_time: Optional[str] = None
    end_time: Optional[str] = None
    updated_at: Optional[str] = None

    # HITL fields
    prompt: Optional[str] = None
    pause: bool = False
    user_input: Optional[str] = None
    control_action: Optional[str] = None  # resume, abort (engine-level control at Hold)

    # Business/results
    result: Optional[str] = None
    bag: Dict[str, Any] = field(default_factory=dict)
    decisions: Dict[str, Any] = field(default_factory=dict)

    # Step-by-step audit trail
    steps_history: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["updated_at"] = now_iso()
        return d

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "WorkflowState":
        return WorkflowState(**d)

# ==========
# In-memory store (POC “long-term” memory)
# ==========
class InMemoryStore:
    def __init__(self):
        self._states: Dict[str, Dict[str, Any]] = {}
        self._events: Dict[str, List[Dict[str, Any]]] = {}

    # State CRUD
    def upsert_state(self, s: WorkflowState) -> None:
        self._states[s.instance_id] = s.to_dict()

    def get_state(self, instance_id: str) -> Optional[WorkflowState]:
        d = self._states.get(instance_id)
        return WorkflowState.from_dict(d) if d else None

    def list_states(self, **filters) -> List[WorkflowState]:
        out = []
        for d in self._states.values():
            match = True
            for k, v in filters.items():
                if v is None:
                    continue
                if d.get(k) != v:
                    match = False
                    break
            if match:
                out.append(WorkflowState.from_dict(d))
        out.sort(key=lambda s: s.updated_at or "", reverse=True)
        return out

    # Events (append-only audit log)
    def append_event(self, instance_id: str, event: str, node: Optional[str], status: str, actor: Optional[str], data: Dict[str, Any]) -> None:
        e = {
            "ts": now_iso(),
            "instance_id": instance_id,
            "event": event,
            "node": node,
            "status": status,
            "actor": actor,
            "data": data,
        }
        self._events.setdefault(instance_id, []).append(e)

    def get_events(self, instance_id: str) -> List[Dict[str, Any]]:
        return self._events.get(instance_id, [])

# ==========
# Workflow definition: Claim flow (based on your diagram)
# ==========
def build_claim_workflow():
    def validate_request(s: Dict[str, Any]) -> Dict[str, Any]:
        s["status"] = "in_progress"
        s["start_time"] = s.get("start_time") or now_iso()
        s["last_node"] = "Validate Request"

        if "validate" not in s["decisions"]:
            if s.get("user_input"):
                ans = norm(s["user_input"])
                if ans in {"yes", "no"}:
                    s["decisions"]["validate"] = ans
                    s["user_input"] = None  # consume
                else:
                    s["pause"] = True
                    s["status"] = "paused"
                    s["last_pause_time"] = now_iso()
                    s["prompt"] = "Invalid input. Validate request? (yes/no)"
            else:
                s["pause"] = True
                s["status"] = "paused"
                s["last_pause_time"] = now_iso()
                s["prompt"] = "Validate request? (yes/no)"
        return s

    def gather_claim_info(s: Dict[str, Any]) -> Dict[str, Any]:
        s["last_node"] = "Gather Claim Info"
        s["bag"].setdefault("claim_details", None)

        if s["bag"]["claim_details"] is None:
            if s.get("user_input"):
                s["bag"]["claim_details"] = s["user_input"]
                s["user_input"] = None  # consume
            else:
                s["pause"] = True
                s["status"] = "paused"
                s["last_pause_time"] = now_iso()
                s["prompt"] = "Provide claim details (free text)."
        return s

    def identify_accounts_and_decide(s: Dict[str, Any]) -> Dict[str, Any]:
        s["last_node"] = "Identify Accounts & Process Decision"
        if "process_decision" not in s["decisions"]:
            if s.get("user_input"):
                d = norm(s["user_input"])
                if d in {"cancel", "hold", "suppress"}:
                    s["decisions"]["process_decision"] = d
                    s["user_input"] = None  # consume
                else:
                    s["pause"] = True
                    s["status"] = "paused"
                    s["last_pause_time"] = now_iso()
                    s["prompt"] = "Decision? Enter: cancel / hold / suppress"
            else:
                s["pause"] = True
                s["status"] = "paused"
                s["last_pause_time"] = now_iso()
                s["prompt"] = "Decision? Enter: cancel / hold / suppress"
        return s

    def cancel_cwd_request(s: Dict[str, Any]) -> Dict[str, Any]:
        s["last_node"] = "Cancel CWD Request"
        s["status"] = "aborted"
        s["end_time"] = now_iso()
        s["result"] = "Workflow aborted."
        return s

    def hold_request(s: Dict[str, Any]) -> Dict[str, Any]:
        s["last_node"] = "Hold Request"
        # Engine-level control while on hold
        action = norm(s.get("control_action"))
        if action == "resume":
            s["control_action"] = None
            s["status"] = "in_progress"
            s["pause"] = False
            s["prompt"] = None
            return s
        if action == "abort":
            s["control_action"] = None
            s["status"] = "aborted"
            s["end_time"] = now_iso()
            s["result"] = "Aborted from hold."
            return s

        s["status"] = "hold"
        s["pause"] = True
        s["last_pause_time"] = now_iso()
        s["prompt"] = "Workflow on hold. Command: resume / abort"
        return s

    def apply_temp_suppression(s: Dict[str, Any]) -> Dict[str, Any]:
        s["last_node"] = "Apply Temporary Suppression"
        if "proceed_fulfill" not in s["decisions"]:
            if s.get("user_input"):
                ans = norm(s["user_input"])
                if ans in {"yes", "no"}:
                    s["decisions"]["proceed_fulfill"] = ans
                    s["user_input"] = None  # consume
                else:
                    s["pause"] = True
                    s["status"] = "paused"
                    s["last_pause_time"] = now_iso()
                    s["prompt"] = "Proceed to fulfill? (yes/no)"
            else:
                s["pause"] = True
                s["status"] = "paused"
                s["last_pause_time"] = now_iso()
                s["prompt"] = "Proceed to fulfill? (yes/no)"
        return s

    def fulfill_case_and_detect(s: Dict[str, Any]) -> Dict[str, Any]:
        s["last_node"] = "Fulfill Case and Detect"
        s["status"] = "completed"
        s["end_time"] = now_iso()
        s["result"] = "Fulfilled and detection complete."
        return s

    g = StateGraph(dict)
    g.add_node("Validate Request", validate_request)
    g.add_node("Gather Claim Info", gather_claim_info)
    g.add_node("Identify Accounts & Process Decision", identify_accounts_and_decide)
    g.add_node("Cancel CWD Request", cancel_cwd_request)
    g.add_node("Hold Request", hold_request)
    g.add_node("Apply Temporary Suppression", apply_temp_suppression)
    g.add_node("Fulfill Case and Detect", fulfill_case_and_detect)

    g.set_entry_point("Validate Request")

    # Conditional transitions — END when waiting for HITL
    g.add_conditional_edges(
        "Validate Request",
        lambda s: (
            "Gather Claim Info" if s["decisions"].get("validate") == "yes"
            else "Cancel CWD Request" if s["decisions"].get("validate") == "no"
            else END
        ),
        {
            "Gather Claim Info": "Gather Claim Info",
            "Cancel CWD Request": "Cancel CWD Request",
            END: END,
        }
    )

    g.add_conditional_edges(
        "Gather Claim Info",
        lambda s: "Identify Accounts & Process Decision" if s["bag"].get("claim_details") else END,
        {
            "Identify Accounts & Process Decision": "Identify Accounts & Process Decision",
            END: END,
        }
    )

    g.add_conditional_edges(
        "Identify Accounts & Process Decision",
        lambda s: (
            "Cancel CWD Request" if s["decisions"].get("process_decision") == "cancel"
            else "Hold Request" if s["decisions"].get("process_decision") == "hold"
            else "Apply Temporary Suppression" if s["decisions"].get("process_decision") == "suppress"
            else END
        ),
        {
            "Cancel CWD Request": "Cancel CWD Request",
            "Hold Request": "Hold Request",
            "Apply Temporary Suppression": "Apply Temporary Suppression",
            END: END,
        }
    )

    # Hold transitions only proceed on explicit control_action=resume/abort; otherwise END
    g.add_conditional_edges(
        "Hold Request",
        lambda s: (
            "Apply Temporary Suppression" if norm(s.get("control_action")) == "resume"
            else "Cancel CWD Request" if norm(s.get("control_action")) == "abort"
            else END
        ),
        {
            "Apply Temporary Suppression": "Apply Temporary Suppression",
            "Cancel CWD Request": "Cancel CWD Request",
            END: END,
        }
    )

    g.add_conditional_edges(
        "Apply Temporary Suppression",
        lambda s: (
            "Fulfill Case and Detect" if s["decisions"].get("proceed_fulfill") == "yes"
            else "Cancel CWD Request" if s["decisions"].get("proceed_fulfill") == "no"
            else END
        ),
        {
            "Fulfill Case and Detect": "Fulfill Case and Detect",
            "Cancel CWD Request": "Cancel CWD Request",
            END: END,
        }
    )

    g.add_edge("Fulfill Case and Detect", END)
    g.add_edge("Cancel CWD Request", END)

    return g.compile()

# ==========
# Engine
# ==========
class Engine:
    def __init__(self, store: InMemoryStore, workflow_name: str = "ClaimWorkflow"):
        self.store = store
        self.workflow_name = workflow_name
        self.graph = build_claim_workflow()

    # ---- lifecycle ----
    def start(self, customer_id: str, started_by: str, bag: Optional[Dict[str, Any]] = None) -> Tuple[str, WorkflowState]:
        instance_id = str(uuid.uuid4())
        s = WorkflowState(
            instance_id=instance_id,
            customer_id=customer_id,
            workflow_name=self.workflow_name,
            started_by=started_by,
            last_actor=started_by,
            bag=bag or {},
        )
        self.store.append_event(instance_id, "created", None, s.status, started_by, {"customer_id": customer_id})
        updated = self._run(s, actor=started_by)
        return instance_id, updated

    def resume(
        self,
        instance_id: str,
        actor: str,
        user_input: Optional[str] = None,
        control_action: Optional[str] = None,
        bag_updates: Optional[Dict[str, Any]] = None,
    ) -> WorkflowState:
        s = self.store.get_state(instance_id)
        if not s:
            raise ValueError(f"No workflow found: {instance_id}")

        # Apply inputs
        s.last_actor = actor
        s.pause = False
        s.prompt = None
        if s.status in {"paused", "hold"}:
            s.status = "in_progress"
        s.last_resume_time = now_iso()
        if user_input is not None:
            s.user_input = user_input
        if control_action is not None:
            s.control_action = norm(control_action)
        if bag_updates:
            s.bag.update(bag_updates)

        self.store.append_event(instance_id, "resume_command", s.last_node, s.status, actor, {
            "user_input": user_input, "control_action": control_action, "bag_updates": bag_updates or {}
        })

        return self._run(s, actor=actor)

    # ---- internal run ----
    def _run(self, state: WorkflowState, actor: str) -> WorkflowState:
        before_node = state.last_node
        out = self.graph.invoke(state.to_dict())
        new_state = WorkflowState.from_dict(out)
        new_state.last_actor = actor

        # Record step transitions and timestamps
        self._record_step_transition(before_node, new_state, actor)

        # Classify event
        if new_state.status == "completed":
            evt = "completed"
        elif new_state.status == "aborted":
            evt = "aborted"
        elif new_state.status == "hold":
            evt = "hold"
        elif new_state.pause:
            evt = "paused"
        else:
            evt = "progressed"

        # Persist
        self.store.append_event(new_state.instance_id, evt, new_state.last_node, new_state.status, actor, {
            "prompt": new_state.prompt, "result": new_state.result
        })
        self.store.upsert_state(new_state)
        return new_state

    def _record_step_transition(self, before_node: Optional[str], s: WorkflowState, actor: str) -> None:
        last_step = s.steps_history[-1]["node"] if s.steps_history else None
        if s.last_node and s.last_node != last_step:
            s.steps_history.append({
                "ts": now_iso(),
                "node": s.last_node,
                "status": s.status,
                "actor": actor,
            })
            # Timestamp hygiene
            if s.pause and s.status in {"paused", "hold"}:
                s.last_pause_time = now_iso()
            if not s.end_time and s.status in {"completed", "aborted"}:
                s.end_time = now_iso()
            if not s.start_time and s.last_node:
                s.start_time = now_iso()

    # ---- queries ----
    def get(self, instance_id: str) -> Optional[WorkflowState]:
        return self.store.get_state(instance_id)

    def history(self, instance_id: str) -> List[Dict[str, Any]]:
        return self.store.get_events(instance_id)

    def list_instances(
        self,
        customer_id: Optional[str] = None,
        status: Optional[str] = None,
        started_by: Optional[str] = None,
        workflow_name: Optional[str] = None,
    ) -> List[WorkflowState]:
        return self.store.list_states(
            customer_id=customer_id,
            status=status,
            started_by=started_by,
            workflow_name=workflow_name or self.workflow_name,
        )

# ==========
# Demo (run this file directly)
# ==========
if __name__ == "__main__":
    store = InMemoryStore()
    engine = Engine(store, workflow_name="ClaimWorkflow")

    # User A starts a workflow for Customer C1
    inst1, s1 = engine.start(customer_id="C1", started_by="user_a")
    print("Start:", s1.status, "| node:", s1.last_node, "| prompt:", s1.prompt)

    # Different user (User B) validates 'yes'
    s1 = engine.resume(inst1, actor="user_b", user_input="yes")
    print("After validate:", s1.status, "| node:", s1.last_node, "| prompt:", s1.prompt)

    # Provide claim details (User C)
    s1 = engine.resume(inst1, actor="user_c", user_input="Claim for disputed withdrawal.")
    print("After claim info:", s1.status, "| node:", s1.last_node, "| prompt:", s1.prompt)

    # Decide to hold (User D)
    s1 = engine.resume(inst1, actor="user_d", user_input="hold")
    print("On hold:", s1.status, "| node:", s1.last_node, "| prompt:", s1.prompt)

    # From hold, resume (User E)
    s1 = engine.resume(inst1, actor="user_e", control_action="resume")
    print("Resumed from hold:", s1.status, "| node:", s1.last_node, "| prompt:", s1.prompt)

    # Apply temp suppression → decide proceed to fulfill (User F)
    s1 = engine.resume(inst1, actor="user_f", user_input="yes")
    print("Completed:", s1.status, "| node:", s1.last_node, "| result:", s1.result)

    # Start another instance (User A) for Customer C2 and abort path
    inst2, s2 = engine.start(customer_id="C2", started_by="user_a")
    print("Start #2:", s2.status, "| node:", s2.last_node, "| prompt:", s2.prompt)
    s2 = engine.resume(inst2, actor="user_b", user_input="no")
    print("Cancelled early:", s2.status, "| node:", s2.last_node, "| result:", s2.result)

    # List and history
    paused_instances = engine.list_instances(status="paused")
    print("Paused instances:", [p.instance_id for p in paused_instances])

    print("History inst1:")
    for ev in engine.history(inst1):
        print("  ", ev)

    print("Steps history inst1:")
    for st in engine.get(inst1).steps_history:
        print("  ", st)
