"""Content-free RAG execution traces. No prompts, queries, credentials or raw IDs."""
import asyncio
import functools
import hashlib
import hmac
import inspect
import json
import logging
import math
import secrets
import time
from contextlib import contextmanager
from contextvars import ContextVar

_ACTIVE = ContextVar('rag_trace', default=None)
_KEY = secrets.token_bytes(32)  # process-local references, never reversible/raw IDs
_LOG = logging.getLogger('career_ai.rag.trace')
_STAGES = frozenset({'request','import','parse','chunk','embedding','embedding_batch',
                    'query_embedding','search','filter','retrieval','retriever_setup',
                    'context','generation','outcome','vector_search','candidate_filter'})
_COUNTS = frozenset({'count','chars','dimension','embedded','skipped','saved','candidates','context_lines'})


def reference(kind, value):
    if type(value) is not int or value < 0:
        return None
    return hmac.new(_KEY, f'{kind}:{value}'.encode(), hashlib.sha256).hexdigest()[:20]


def _error_code(exc):
    # Closed classification: never serialize exception text, repr or dynamic class names.
    name = type(exc).__name__
    if isinstance(exc, asyncio.CancelledError): return 'cancelled'
    if isinstance(exc, TimeoutError): return 'timeout'
    if 'Config' in name or 'Input' in name or 'Dimension' in name: return 'invalid_configuration_or_input'
    if 'Unavailable' in name: return 'unavailable'
    if 'Parse' in name or 'Format' in name: return 'parse_failed'
    if 'TooLarge' in name: return 'input_too_large'
    return 'execution_failed'


class Trace:
    def __init__(self):
        self.id = secrets.token_hex(12)
        self.sequence = 0
        self.context_rendered = False
        self.context_submissions = 0
        self.generation_error = None
        self.context_digest = None

    def emit(self, stage, status, elapsed_ms=0, **fields):
        try:
            if stage not in _STAGES or status not in {'ok','failed','empty','skipped','cancelled'}:
                return
            self.sequence += 1
            event = {'event':'rag_stage','trace_id':self.id,'sequence':self.sequence,
                     'stage':stage,'status':status,'elapsed_ms':round(max(0,elapsed_ms),3)}
            for key in _COUNTS:
                value = fields.get(key)
                if type(value) is int and value >= 0: event[key] = value
            for key in ('context_rendered','context_submitted','generation_ok','rag_requested'):
                if type(fields.get(key)) is bool: event[key] = fields[key]
            if fields.get('failure_code') in {'cancelled','timeout','invalid_configuration_or_input',
                 'unavailable','parse_failed','input_too_large','execution_failed','business_failed',
                 'boundary','mode','auth','rate_limit','response','config','ai_error'}:
                event['failure_code'] = fields['failure_code']
            if stage == 'retrieval':
                event['rerank'] = 'not_supported'
            # These fields are produced internally from integers; never accept free-form metadata.
            if 'document_id' in fields:
                event['document_ref'] = reference('document', fields['document_id'])
            if 'results' in fields:
                safe=[]
                for item in fields['results'][:20]:
                    meta = getattr(item,'metadata',{}) or {}
                    cid = getattr(item,'chunk_id',None)
                    did = getattr(item,'document_id',None)
                    if isinstance(meta,dict):
                        cid = cid if cid is not None else meta.get('chunk_id')
                        did = did if did is not None else meta.get('document_id')
                    score = getattr(item,'score',None)
                    if score is None and isinstance(meta,dict): score=meta.get('score')
                    row={'chunk_ref':reference('chunk',cid),'document_ref':reference('document',did)}
                    if type(score) in (int,float) and math.isfinite(score): row['score']=round(score,6)
                    safe.append(row)
                event['results']=safe
                event['results_truncated']=len(fields['results'])>20
            _LOG.info(json.dumps(event,ensure_ascii=True))
        except Exception:
            # Observability must never break a business operation, even if a handler fails.
            pass


@contextmanager
def trace_scope():
    existing=_ACTIVE.get()
    if existing is not None:
        yield existing
        return
    trace=Trace();token=_ACTIVE.set(trace)
    try: yield trace
    finally: _ACTIVE.reset(token)


@contextmanager
def span(stage):
    with trace_scope() as trace:
        fields={};started=time.perf_counter()
        try:
            yield fields
        except BaseException as exc:
            trace.emit(stage,'cancelled' if isinstance(exc,asyncio.CancelledError) else 'failed',
                       (time.perf_counter()-started)*1000,failure_code=_error_code(exc))
            raise
        else:
            status=fields.pop('_status','ok')
            trace.emit(stage,status,(time.perf_counter()-started)*1000,**fields)


def context_ready(lines):
    trace=_ACTIVE.get()
    if trace is not None: trace.context_rendered=bool(lines)


def _prompt_digest(prompt):
    return hmac.new(_KEY, prompt.encode('utf-8'), hashlib.sha256).digest()


def context_submitted(prompt):
    """Called immediately before the actual provider transport invocation."""
    trace=_ACTIVE.get()
    if trace is not None and trace.context_digest == _prompt_digest(prompt):
        trace.context_submissions+=1


def generation_error(kind):
    trace = _ACTIVE.get()
    if trace is not None:
        trace.generation_error = kind


def _summarize(stage,result,fields,trace,submissions):
    if stage in {'retrieval','search','filter','vector_search','chunk','embedding_batch'}:
        fields['count']=len(result)
        if not result: fields['_status']='empty'
        if stage in {'retrieval','search','filter'}: fields['results']=result
    elif stage=='query_embedding': fields['dimension']=len(result)
    elif stage=='embedding':
        vectors,failure=result
        fields['embedded']=len(vectors)
        if failure is not None:
            fields['_status']='failed';fields['failure_code']=_error_code(failure[1])
    elif stage=='import':
        fields.update(document_id=result.get('document_id'),count=result.get('chunk_count'),
                      embedded=result.get('embedded_chunks'),saved=result.get('saved_chunks'),skipped=result.get('skipped_chunks'))
        if not result.get('ok'): fields.update(_status='failed',failure_code='business_failed')
    elif stage=='outcome':
        if isinstance(result,dict):
            fields['generation_ok']=bool(result.get('ok'))
            if not result.get('ok'): fields.update(_status='failed',failure_code='business_failed')
        fields['context_rendered']=trace.context_rendered
        fields['context_submitted']=trace.context_submissions>submissions
    elif stage=='context':
        fields['context_rendered']=trace.context_rendered
        fields['chars']=len(result[1])
        trace.context_digest = _prompt_digest(result[1]) if trace.context_rendered else None
    elif stage=='generation':
        failed=not isinstance(result,str) or not result.strip() or result.lstrip().startswith(('API调用失败','SparkAPI not available','AI 回答为空'))
        fields.update(generation_ok=not failed,
                      context_submitted=trace.context_submissions>submissions)
        if failed: fields.update(_status='failed',failure_code=trace.generation_error or 'business_failed')


def observed(stage):
    """Observe actual invocation/result; preserve signature, values and exceptions."""
    def decorate(fn):
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args,**kwargs):
                with trace_scope() as trace, span(stage) as fields:
                    submissions=trace.context_submissions
                    if stage == 'generation':
                        trace.generation_error = None
                        prompt = kwargs.get('message', args[1] if len(args)>1 else '')
                        fields['context_rendered'] = (isinstance(prompt,str) and
                            trace.context_digest == _prompt_digest(prompt))
                    if stage == 'outcome' and type(kwargs.get('use_rag')) is bool:
                        fields['rag_requested'] = kwargs['use_rag']
                    result=await fn(*args,**kwargs)
                    try: _summarize(stage,result,fields,trace,submissions)
                    except Exception: pass
                    return result
            return async_wrapper
        @functools.wraps(fn)
        def wrapper(*args,**kwargs):
            with trace_scope() as trace, span(stage) as fields:
                submissions=trace.context_submissions
                result=fn(*args,**kwargs)
                try: _summarize(stage,result,fields,trace,submissions)
                except Exception: pass
                return result
        return wrapper
    return decorate
