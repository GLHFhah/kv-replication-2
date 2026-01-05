import hashlib
import json
import copy
from typing import List, Dict, Any, Optional
from anysystem import Context, Message, Process

VersionVector = Dict[str, int]

def vv_from_str(s: Optional[str]) -> VersionVector:
    if not s:
        return {}
    try:
        return json.loads(s)
    except:
        return {}

def vv_to_str(vv: VersionVector) -> str:
    return json.dumps(vv, sort_keys=True)

def compare_vectors(v1: VersionVector, v2: VersionVector) -> Optional[int]:
    keys = set(v1.keys()) | set(v2.keys())
    has_greater = False
    has_less = False
    
    for k in keys:
        c1 = v1.get(k, 0)
        c2 = v2.get(k, 0)
        if c1 > c2:
            has_greater = True
        elif c1 < c2:
            has_less = True
            
    if has_greater and has_less:
        return None
    if has_greater:
        return 1
    if has_less:
        return -1
    return 0

def merge_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not entries:
        return []
        
    unique_entries = []
    seen = set()
    for e in entries:
        sig = (e['value'], tuple(sorted(e['context'].items())))
        if sig not in seen:
            seen.add(sig)
            unique_entries.append(e)
            
    res = []
    for i, e1 in enumerate(unique_entries):
        dominated = False
        for j, e2 in enumerate(unique_entries):
            if i == j: continue
            cmp = compare_vectors(e2['context'], e1['context'])
            if cmp == 1:
                dominated = True
                break
        if not dominated:
            res.append(e1)
    return res

def merge_cart_values(values: List[str]) -> str:
    items = set()
    for v in values:
        if v:
            for item in v.split(','):
                if item:
                    items.add(item.strip())
    return ",".join(sorted(list(items)))

class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = nodes
        self._data: Dict[str, List[Dict[str, Any]]] = {}
        self._req_counter = 0
        self._pending_ops = {}
        self._pending_forwards = {}
        
    def _get_replicas(self, key: str) -> List[str]:
        indices = get_key_replicas(key, len(self._nodes))
        return [self._nodes[i] for i in indices]

    def _get_preference_list(self, key: str) -> List[str]:
        node_count = len(self._nodes)
        result = []
        key_hash = int.from_bytes(hashlib.md5(key.encode('utf8')).digest(), 'little', signed=False)
        cur = key_hash % node_count
        for _ in range(node_count):
            result.append(self._nodes[cur])
            cur = (cur + 1) % node_count
        return result

    def on_local_message(self, msg: Message, ctx: Context):
        req_id = f"loc_{self._id}_{self._req_counter}"
        self._req_counter += 1
        
        if msg.type == 'GET':
            key = msg['key']
            quorum = msg['quorum']
            pref_list = self._get_preference_list(key)
            
            self._pending_ops[req_id] = {
                'type': 'GET',
                'key': key,
                'quorum': quorum,
                'responses': {},
                'pref_list': pref_list,
                'next_idx': 3
            }
            
            for r in pref_list[:3]:
                ctx.send(Message('READ', {'key': key, 'req_id': req_id}), r)
            
            ctx.set_timer(req_id, 0.3)

        elif msg.type == 'PUT':
            key = msg['key']
            value = msg['value']
            context_str = msg['context']
            quorum = msg['quorum']
            replicas = self._get_replicas(key)
            
            if self._id in replicas:
                self._coordinate_write(req_id, key, value, context_str, quorum, ctx, is_local=True)
            else:
                idx = int(self._id) % len(replicas)
                target = replicas[idx]
                
                self._pending_forwards[req_id] = {
                    'key': key,
                    'value': value,
                    'context': context_str,
                    'quorum': quorum,
                    'replicas': replicas,
                    'tried': [target]
                }
                ctx.send(Message('FORWARD_PUT', {
                    'key': key, 
                    'value': value, 
                    'context': context_str, 
                    'quorum': quorum,
                    'req_id': req_id
                }), target)
                ctx.set_timer(req_id, 0.3)

    def _coordinate_write(self, req_id: str, key: str, value: str, context_str: str, quorum: int, ctx: Context, is_local: bool, forward_src: str = None, forward_req_id: str = None):
        base_vv = vv_from_str(context_str)
        new_vv = copy.deepcopy(base_vv)
        new_vv[self._id] = new_vv.get(self._id, 0) + 1
        new_vv_str = vv_to_str(new_vv)
        
        pref_list = self._get_preference_list(key)
        
        self._pending_ops[req_id] = {
            'type': 'PUT',
            'key': key,
            'value': value,
            'new_context': new_vv,
            'new_context_str': new_vv_str,
            'is_local': is_local,
            'forward_src': forward_src,
            'forward_req_id': forward_req_id,
            'quorum': quorum,
            'acks': 0,
            'pref_list': pref_list,
            'next_idx': 3
        }
        
        msg = Message('WRITE', {
            'key': key, 
            'value': value, 
            'context': new_vv_str,
            'req_id': req_id
        })
        for r in pref_list[:3]:
            ctx.send(msg, r)
        
        ctx.set_timer(req_id, 0.3)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'READ':
            key = msg['key']
            req_id = msg['req_id']
            entries = self._data.get(key, [])
            transport = [{'value': e['value'], 'context': vv_to_str(e['context'])} for e in entries]
            ctx.send(Message('READ_RESP', {'req_id': req_id, 'entries': transport}), sender)
            
        elif msg.type == 'READ_RESP':
            req_id = msg['req_id']
            if req_id not in self._pending_ops:
                return
            op = self._pending_ops[req_id]
            if op['type'] != 'GET':
                return
                
            entries = [{'value': e['value'], 'context': vv_from_str(e['context'])} for e in msg['entries']]
            op['responses'][sender] = entries
            
            if len(op['responses']) >= op['quorum']:
                self._complete_get(req_id, op, ctx)

        elif msg.type == 'WRITE':
            key = msg['key']
            value = msg['value']
            new_vv = vv_from_str(msg['context'])
            req_id = msg['req_id']
            
            current = self._data.get(key, [])
            new_entry = {'value': value, 'context': new_vv}
            
            updated = [e for e in current if compare_vectors(e['context'], new_vv) != -1]
            
            is_stale = any(compare_vectors(e['context'], new_vv) == 1 for e in updated)
            
            if not is_stale:
                exists = any(compare_vectors(e['context'], new_vv) == 0 for e in updated)
                if not exists:
                    updated.append(new_entry)
            
            self._data[key] = updated
            
            replicas = self._get_replicas(key)
            if self._id not in replicas:
                for e in updated:
                    for r in replicas:
                        ctx.send(Message('WRITE', {
                            'key': key,
                            'value': e['value'],
                            'context': vv_to_str(e['context']),
                            'req_id': f"hh_{self._id}_{self._req_counter}"
                        }), r)
                        self._req_counter += 1
                ctx.set_timer(f"hh1_{key}", 5.0)
            
            ctx.send(Message('WRITE_RESP', {'req_id': req_id}), sender)

        elif msg.type == 'WRITE_RESP':
            req_id = msg['req_id']
            if req_id not in self._pending_ops:
                return
            op = self._pending_ops[req_id]
            if op['type'] != 'PUT':
                return
                
            op['acks'] += 1
            if op['acks'] >= op['quorum']:
                self._complete_put(req_id, op, ctx)

        elif msg.type == 'FORWARD_PUT':
            fwd_req_id = f"fwd_{self._id}_{self._req_counter}"
            self._req_counter += 1
            self._coordinate_write(
                fwd_req_id, 
                msg['key'], 
                msg['value'], 
                msg['context'], 
                msg['quorum'], 
                ctx,
                is_local=False,
                forward_src=sender,
                forward_req_id=msg['req_id']
            )
            
        elif msg.type == 'FORWARD_PUT_RESP':
            req_id = msg['req_id']
            if req_id in self._pending_forwards:
                del self._pending_forwards[req_id]
                ctx.cancel_timer(req_id)
                ctx.send_local(Message('PUT_RESP', {
                    'key': msg['key'],
                    'values': msg['values'],
                    'context': msg['context']
                }))

    def _complete_get(self, req_id: str, op: dict, ctx: Context):
        del self._pending_ops[req_id]
        ctx.cancel_timer(req_id)
        
        all_entries = []
        for entries in op['responses'].values():
            all_entries.extend(entries)
        
        merged = merge_entries(all_entries)
        
        for node in op['responses'].keys():
            for m in merged:
                ctx.send(Message('WRITE', {
                    'key': op['key'],
                    'value': m['value'],
                    'context': vv_to_str(m['context']),
                    'req_id': f"rr_{self._id}_{self._req_counter}"
                }), node)
                self._req_counter += 1
        
        if op['key'].startswith('CART'):
            if merged:
                vals = [e['value'] for e in merged]
                final_val = merge_cart_values(vals)
                final_vv = {}
                for e in merged:
                    for node, counter in e['context'].items():
                        final_vv[node] = max(final_vv.get(node, 0), counter)
                result_values = [final_val]
                result_context = vv_to_str(final_vv)
            else:
                result_values = []
                result_context = None
        else:
            result_values = [e['value'] for e in merged]
            final_vv = {}
            if merged:
                for e in merged:
                    for node, counter in e['context'].items():
                        final_vv[node] = max(final_vv.get(node, 0), counter)
                result_context = vv_to_str(final_vv)
            else:
                result_context = None
        
        ctx.send_local(Message('GET_RESP', {
            'key': op['key'],
            'values': result_values,
            'context': result_context
        }))

    def _complete_put(self, req_id: str, op: dict, ctx: Context):
        del self._pending_ops[req_id]
        ctx.cancel_timer(req_id)
        
        current = self._data.get(op['key'], [])
        just_written = {'value': op['value'], 'context': op['new_context']}
        merged = merge_entries(current + [just_written])
        
        if op['key'].startswith('CART'):
            vals = [e['value'] for e in merged]
            final_val = merge_cart_values(vals)
            final_values = [final_val]
        else:
            final_values = [e['value'] for e in merged]
        
        final_vv = {}
        for e in merged:
            for node, counter in e['context'].items():
                final_vv[node] = max(final_vv.get(node, 0), counter)
        final_context = vv_to_str(final_vv)
        
        if op['is_local']:
            ctx.send_local(Message('PUT_RESP', {
                'key': op['key'],
                'values': final_values,
                'context': final_context
            }))
        elif op['forward_src']:
            ctx.send(Message('FORWARD_PUT_RESP', {
                'key': op['key'],
                'values': final_values,
                'context': final_context,
                'req_id': op['forward_req_id']
            }), op['forward_src'])

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name.startswith("hh1_"):
            key = timer_name[4:]
            if key in self._data:
                replicas = self._get_replicas(key)
                if self._id not in replicas:
                    entries = self._data[key]
                    for e in entries:
                        for r in replicas:
                            ctx.send(Message('WRITE', {
                                'key': key,
                                'value': e['value'],
                                'context': vv_to_str(e['context']),
                                'req_id': f"hhr_{self._id}_{self._req_counter}"
                            }), r)
                            self._req_counter += 1
            return

        if timer_name in self._pending_ops:
            op = self._pending_ops[timer_name]
            
            progress = len(op['responses']) if op['type'] == 'GET' else op['acks']
            
            if progress < op['quorum'] and op['next_idx'] < len(op['pref_list']):
                target = op['pref_list'][op['next_idx']]
                op['next_idx'] += 1
                
                if op['type'] == 'GET':
                    ctx.send(Message('READ', {'key': op['key'], 'req_id': timer_name}), target)
                else:
                    ctx.send(Message('WRITE', {
                        'key': op['key'], 
                        'value': op['value'], 
                        'context': op['new_context_str'],
                        'req_id': timer_name
                    }), target)
                
                ctx.set_timer(timer_name, 0.3)
            else:
                del self._pending_ops[timer_name]

        if timer_name in self._pending_forwards:
            fwd = self._pending_forwards[timer_name]
            remaining = [r for r in fwd['replicas'] if r not in fwd['tried']]
            if remaining:
                target = remaining[0]
                fwd['tried'].append(target)
                ctx.send(Message('FORWARD_PUT', {
                    'key': fwd['key'], 
                    'value': fwd['value'], 
                    'context': fwd['context'], 
                    'quorum': fwd['quorum'],
                    'req_id': timer_name
                }), target)
                ctx.set_timer(timer_name, 0.3)
            else:
                del self._pending_forwards[timer_name]

def get_key_replicas(key: str, node_count: int):
    replicas = []
    key_hash = int.from_bytes(hashlib.md5(key.encode('utf8')).digest(), 'little', signed=False)
    cur = key_hash % node_count
    for _ in range(3):
        replicas.append(cur)
        cur = get_next_replica(cur, node_count)
    return replicas

def get_next_replica(i, node_count: int):
    return (i + 1) % node_count