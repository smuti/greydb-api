"""
Skorjin Router - Skorjin konuşmaları ve feedback işlemleri
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

from app.services.db import query_to_df

router = APIRouter(tags=["skorjin"])


class ConversationCreate(BaseModel):
    """Konuşma kaydetme şeması"""
    user_id: str
    user_email: Optional[str] = None
    user_message: str
    skorjin_response: str


class ConversationResponse(BaseModel):
    """Konuşma response şeması"""
    id: int
    user_id: str
    user_email: Optional[str]
    user_message: str
    skorjin_response: str
    created_at: datetime


class MessageFeedbackCreate(BaseModel):
    """Mesaj feedback şeması"""
    conversation_id: int
    user_id: str
    feedback_type: str  # 'up' veya 'down'


class MessageFeedbackResponse(BaseModel):
    """Feedback response şeması"""
    id: int
    content_type: str
    content_id: str
    user_id: str
    feedback_type: str
    created_at: datetime


@router.post("/skorjin/conversations", response_model=ConversationResponse)
async def save_conversation(conversation: ConversationCreate):
    """Skorjin konuşmasını kaydet"""
    sql = """
        INSERT INTO greydb.skorjin_conversations (
            user_id, user_email, user_message, skorjin_response
        ) VALUES (%s, %s, %s, %s)
        RETURNING *
    """
    
    df = query_to_df(sql, (
        conversation.user_id,
        conversation.user_email,
        conversation.user_message,
        conversation.skorjin_response
    ), commit=True)
    
    if df.empty:
        raise HTTPException(status_code=500, detail="Konuşma kaydedilemedi")
    
    row = df.iloc[0]
    return _row_to_conversation_response(row)


@router.get("/skorjin/conversations", response_model=List[ConversationResponse])
async def list_conversations(
    user_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Skorjin konuşmalarını listele"""
    if user_id:
        sql = """
            SELECT * FROM greydb.skorjin_conversations
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        df = query_to_df(sql, (user_id, limit, offset))
    else:
        sql = """
            SELECT * FROM greydb.skorjin_conversations
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        df = query_to_df(sql, (limit, offset))
    
    conversations = []
    for _, row in df.iterrows():
        conversations.append(_row_to_conversation_response(row))
    
    return conversations


@router.get("/skorjin/conversations/stats")
async def get_conversation_stats():
    """Konuşma istatistikleri"""
    sql = """
        SELECT 
            COUNT(*) as total_conversations,
            COUNT(DISTINCT user_id) as unique_users,
            DATE(created_at) as date,
            COUNT(*) as daily_count
        FROM greydb.skorjin_conversations
        GROUP BY DATE(created_at)
        ORDER BY date DESC
        LIMIT 30
    """
    df = query_to_df(sql)
    
    total_sql = """
        SELECT 
            COUNT(*) as total_conversations,
            COUNT(DISTINCT user_id) as unique_users
        FROM greydb.skorjin_conversations
    """
    total_df = query_to_df(total_sql)
    
    daily_stats = []
    for _, row in df.iterrows():
        daily_stats.append({
            "date": str(row["date"]),
            "count": int(row["daily_count"])
        })
    
    return {
        "total_conversations": int(total_df.iloc[0]["total_conversations"]) if not total_df.empty else 0,
        "unique_users": int(total_df.iloc[0]["unique_users"]) if not total_df.empty else 0,
        "daily_stats": daily_stats
    }


@router.post("/skorjin/feedback", response_model=MessageFeedbackResponse)
async def save_message_feedback(feedback: MessageFeedbackCreate):
    """Skorjin mesajı için feedback kaydet (mevcut feedbacks tablosunu kullanır)"""
    content_id = str(feedback.conversation_id)
    content_type = "skorjin_message"
    
    # Mevcut feedback kontrol et
    check_sql = """
        SELECT id, feedback_type FROM greydb.feedbacks
        WHERE content_type = %s AND content_id = %s AND user_id = %s
    """
    existing_df = query_to_df(check_sql, (content_type, content_id, feedback.user_id))
    
    if not existing_df.empty:
        existing_id = existing_df.iloc[0]['id']
        existing_type = existing_df.iloc[0]['feedback_type']
        
        if existing_type == feedback.feedback_type:
            # Aynı feedback tekrarlandıysa sil (toggle off)
            delete_sql = "DELETE FROM greydb.feedbacks WHERE id = %s"
            query_to_df(delete_sql, (existing_id,), commit=True)
            raise HTTPException(status_code=200, detail="Feedback removed")
        else:
            # Farklı feedback verdiyse güncelle
            update_sql = """
                UPDATE greydb.feedbacks SET feedback_type = %s
                WHERE id = %s RETURNING *
            """
            updated_df = query_to_df(update_sql, (feedback.feedback_type, existing_id), commit=True)
            return _row_to_feedback_response(updated_df.iloc[0])
    else:
        # Yeni feedback ekle
        insert_sql = """
            INSERT INTO greydb.feedbacks (content_type, content_id, user_id, feedback_type)
            VALUES (%s, %s, %s, %s) RETURNING *
        """
        inserted_df = query_to_df(insert_sql, (content_type, content_id, feedback.user_id, feedback.feedback_type), commit=True)
        return _row_to_feedback_response(inserted_df.iloc[0])


@router.get("/skorjin/feedback/{conversation_id}")
async def get_message_feedback_counts(conversation_id: int):
    """Belirli bir konuşma için feedback sayıları"""
    content_id = str(conversation_id)
    content_type = "skorjin_message"
    
    sql = """
        SELECT 
            COALESCE(SUM(CASE WHEN feedback_type = 'up' THEN 1 ELSE 0 END), 0) AS up_votes,
            COALESCE(SUM(CASE WHEN feedback_type = 'down' THEN 1 ELSE 0 END), 0) AS down_votes
        FROM greydb.feedbacks
        WHERE content_type = %s AND content_id = %s
    """
    df = query_to_df(sql, (content_type, content_id))
    
    if df.empty:
        return {"conversation_id": conversation_id, "up_votes": 0, "down_votes": 0}
    
    return {
        "conversation_id": conversation_id,
        "up_votes": int(df.iloc[0]["up_votes"]),
        "down_votes": int(df.iloc[0]["down_votes"])
    }


def _row_to_conversation_response(row) -> dict:
    """DataFrame satırını conversation response'a çevir"""
    import pandas as pd
    
    return {
        "id": int(row["id"]),
        "user_id": row["user_id"],
        "user_email": row["user_email"] if not pd.isna(row["user_email"]) else None,
        "user_message": row["user_message"],
        "skorjin_response": row["skorjin_response"],
        "created_at": row["created_at"]
    }


def _row_to_feedback_response(row) -> dict:
    """DataFrame satırını feedback response'a çevir"""
    return {
        "id": int(row["id"]),
        "content_type": row["content_type"],
        "content_id": row["content_id"],
        "user_id": row["user_id"],
        "feedback_type": row["feedback_type"],
        "created_at": row["created_at"]
    }

