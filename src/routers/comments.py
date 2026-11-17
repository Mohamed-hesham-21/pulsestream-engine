from typing import Literal
from fastapi import APIRouter, HTTPException, Query, Response, status
from fastapi.responses import ORJSONResponse
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
    default_response_class=ORJSONResponse,
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

@router.delete("/comments/{comment_id}", response_model=status.HTTP_204_NO_CONTENT)
def delete_single_comment(comment_id:int):
    deleted = controller.delete_comment(comment_id=comment_id)
    if deleted == None:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='comment not found'
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
