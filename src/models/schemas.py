from pydantic import BaseModel, Field

class CommentCreate(BaseModel): # PUT
    user_id : int
    content : str = Field(min_length=1, max_length=2000)

class CommentReplace(BaseModel): # post
    user_id : int
    content : str = Field(min_length=1, max_length=2000)
    is_pinned: bool = False

class CommentPatch(BaseModel): # patch
    content : str | None = Field(default=None, min_length=1, max_length=2000)
    is_pinned : bool | None = None

class CommentResponse(BaseModel): # response for any call
    comment_id : int
    post_id : int
    parent_comment_id : int | None = None
    user_id : int
    content : str
    is_pinned : bool
    created_at_ms : int