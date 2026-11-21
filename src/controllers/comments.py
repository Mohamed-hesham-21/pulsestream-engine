import bisect
import os
from collections import defaultdict
from src.distributed.snowflake import SnowflakeGenerator
from src.models.schemas import (
    CommentCreate,
    CommentReplace,
    CommentPatch,
    CommentResponse,
)

class CommentController:
    def __init__(self, node_id: int | None = None):
        if node_id is None:
            node_id = os.getpid() % 1024
        self.id_gen = SnowflakeGenerator(node_id=node_id)
        self._store: dict[int, dict] = {}
        self._post_index: dict[int, list[int]] = defaultdict(list)
        self._user_index: dict[int, list[int]] = defaultdict(list)
        self._reply_index: dict[int, list[int]] = defaultdict(list)

    def _paginate_ids(
        self,
        id_list: list[int],
        cursor_id: int | None = None,
        sort: str = "asc",
        limit: int = 20,
    ) -> list[CommentResponse]:
        results : list[CommentResponse] = []

        if not id_list:
            return results

        if sort == 'asc':
            start_idx = bisect.bisect_right(id_list, cursor_id) if cursor_id is not None else 0
            for idx in range(start_idx, len(id_list)):
                cid = id_list[idx]
                if cid in self._store:
                    results.append(CommentResponse(**self._store[cid]))
                    if len(results) == limit:
                        break
        else:
                start_idx = bisect.bisect_left(id_list, cursor_id) - 1 if cursor_id is not None else len(id_list) - 1
                for idx in range(start_idx, -1, -1):
                    cid = id_list[idx]
                    if cid in self._store:
                        results.append(CommentResponse(**self._store[cid]))
                        if len(results) == limit:
                            break
        
        return results        

    def create_comment(self, post_id: int, payload: CommentCreate) -> CommentResponse:
  
        comment_id = self.id_gen.next_id()
        timestamp = self.id_gen.decode(comment_id)['Timestamp']
        record = {
            'comment_id' : comment_id,
            'post_id' : post_id,
            'user_id' : payload.user_id,
            'content' : payload.content,
            'parent_comment_id' : None,
            'is_pinned' : False,
            'created_at_ms' : timestamp
        }
        self._store[comment_id] = record
        self._post_index[post_id].append(comment_id)
        self._user_index[payload.user_id].append(comment_id)

        return CommentResponse(**record)

    def get_post_comments(
        self,
        post_id: int,
        cursor_id: int | None = None,
        sort: str = "asc",
        limit: int = 20,
    ) -> list[CommentResponse]:
        return self._paginate_ids(self._post_index.get(post_id, []), cursor_id=cursor_id, sort=sort, limit=limit)

    def query_comments(
        self,
        post_id: int | None = None,
        user_id: int | None = None,
        cursor_id: int | None = None,
        sort: str = "desc",
        limit: int = 20,
    ) -> list[CommentResponse]:
    
        if post_id is not None and user_id is None:
            return self._paginate_ids(self._post_index.get(post_id, []), cursor_id=cursor_id, sort=sort, limit=limit)

        if post_id is None and user_id is not None:
            return  self._paginate_ids(self._user_index.get(user_id, []), cursor_id=cursor_id, sort=sort, limit=limit)

        if post_id is not None and user_id is not None:
            user_ids = self._user_index.get(user_id, [])
            post_ids = self._post_index.get(post_id, [])
            id_list = []
            if len(user_ids) <= len(post_ids):
                id_list = [
                    cid for cid in user_ids
                    if cid in self._store and self._store[cid]["post_id"] == post_id
                ]
            else:
                id_list = [
                    cid for cid in post_ids
                    if cid in self._store and self._store[cid]["user_id"] == user_id
                ]
            return self._paginate_ids(id_list, cursor_id=cursor_id, sort=sort, limit=limit)
            

        return self._paginate_ids(list(self._store.keys()), cursor_id=cursor_id, sort=sort, limit=limit)

    def get_comment(self, comment_id: int) -> CommentResponse | None:
        record = self._store.get(comment_id)
        return CommentResponse(**record) if record else None

    def replace_comment(self, comment_id: int, payload: CommentReplace) -> CommentResponse | None:
        if comment_id not in self._store: return None
        record = self._store[comment_id]
        if payload.user_id != record["user_id"]:
            self._user_index[record["user_id"]].remove(comment_id)
            bisect.insort(self._user_index[payload.user_id], comment_id)

        record['user_id'] = payload.user_id
        record['content'] = payload.content
        record['is_pinned'] = payload.is_pinned

        self._store[comment_id] = record
        return CommentResponse(**record)


    def patch_comment(self, comment_id: int, payload: CommentPatch) -> CommentResponse | None:
        if comment_id not in self._store:
            return None
        updates = payload.model_dump(exclude_unset=True)
        self._store[comment_id].update(updates)
        return CommentResponse(**self._store[comment_id])
    

    def delete_comment(self, comment_id: int) -> bool:
        if comment_id not in self._store: return False
        self._store.pop(comment_id)
        return True

    def create_reply(self, parent_comment_id: int, payload: CommentCreate) -> CommentResponse | None:
        if parent_comment_id not in self._store: return None
        comment_id = self.id_gen.next_id()
        timestamp = self.id_gen.decode(comment_id)['Timestamp']
        record = {
                    'comment_id' : comment_id,
                    'post_id' : self._store[parent_comment_id]['post_id'],
                    'user_id' : payload.user_id,
                    'content' : payload.content,
                    'parent_comment_id' : parent_comment_id,
                    'is_pinned' : False,
                    'created_at_ms' : timestamp
                }
        self._store[comment_id] = record
        self._reply_index[parent_comment_id].append(comment_id)
        self._user_index[payload.user_id].append(comment_id)

        return CommentResponse(**record)

    
    def get_replies(
        self,
        parent_comment_id: int,
        cursor_id: int | None = None,
        sort: str = "asc",
        limit: int = 20,
    ) -> list[CommentResponse] | None:
        
        if parent_comment_id not in self._store: return None
        return self._paginate_ids(self._reply_index.get(parent_comment_id, []), cursor_id=cursor_id, sort=sort, limit=limit)
        