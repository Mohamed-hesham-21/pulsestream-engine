from typing import Literal
from fastapi import APIRouter, HTTPException, Query, Response, status
from src.controllers.comments import CommentController
from src.models.schemas import (
    CommentCreate,
    CommentReplace,
    CommentPatch,
    CommentResponse,
)

router = APIRouter(
    prefix="/api/v1",
    tags=["comments"],
)

controller = CommentController()


@router.post(
    "/posts/{post_id}/comments",
    response_model=CommentResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_post_comment(post_id: int, payload: CommentCreate):
    return controller.create_comment(post_id=post_id, payload=payload)

@router.get(
    "/comments/{comment_id}",
    response_model=CommentResponse
)
async def get_single_comment(comment_id : int):
    response = controller.get_comment(comment_id=comment_id)
    if response is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="comment not found"
        )
    return response

@router.get(
        "/posts/{post_id}/comments",
        response_model=list[CommentResponse])
async def list_post_comments(post_id: int,
    cursor_id: int | None = None,
    sort: Literal["asc", "desc"] = "asc",
    limit: int = Query(default=20, ge=1, le=100)):
    response = controller.get_post_comments(post_id=post_id, cursor_id=cursor_id, sort=sort, limit=limit)
    if response == None:
        raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='post not found'
            )
    return response
        

@router.get(
        "/comments",
        response_model=list[CommentResponse]
)
async def list_all_comments(post_id: int | None = None,
    user_id: int | None = None,
    cursor_id: int | None = None,
    sort: Literal["asc", "desc"] = "desc",
    limit: int = Query(default=20, ge=1, le=100)):
    response = controller.query_comments(post_id=post_id, user_id=user_id, cursor_id=cursor_id, sort=sort, limit=limit)
    if response == None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='No comments found'
        )
    return response


@router.put('/comments/{comment_id}', response_model=CommentResponse)
async def replace_single_comment(comment_id: int, payload: CommentReplace):
    response = controller.replace_comment(comment_id=comment_id, payload=payload)
    if response == None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='comment not found'
        )
    return response

@router.patch('/comments/{comment_id}', response_model=CommentResponse)
async def patch_single_comment(comment_id: int, payload: CommentPatch):
    response = controller.patch_comment(comment_id=comment_id, payload=payload)
    if response == None:
        raise HTTPException(
            status_code= status.HTTP_404_NOT_FOUND,
            detail='comment not found'
        )
    return response

@router.delete("/comments/{comment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_single_comment(comment_id:int):
    deleted = controller.delete_comment(comment_id=comment_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='comment not found'
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@router.post('/comments/{comment_id}/replies', response_model=CommentResponse,status_code=status.HTTP_201_CREATED)
async def create_comment_reply(comment_id: int, payload: CommentCreate):
    response =  controller.create_reply(parent_comment_id=comment_id,payload=payload)
    if not response:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='comment not found'
        )
    return response

@router.get(
    '/comments/{comment_id}/replies',
    response_model=list[CommentResponse]
)
async def list_comment_replies(
    comment_id:int,
    cursor_id: int | None = None, 
    sort: Literal['asc', 'desc'] = "asc",
    limit: int = Query(default=20, le=100, ge=1)
):
    response = controller.get_replies(parent_comment_id=comment_id, cursor_id=cursor_id, sort=sort, limit=limit)
    if response == None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='comment not found'
        )
    return response
