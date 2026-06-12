# Franklink: The First AI Professional Network

**The first chat that becomes your startup.**
**The first chat where interview questions actually get shared.**
**The first chat with the people grinding finals with you at 2 a.m.**
**The first chat with strangers who turn into your AI conference crew.**

## Features

| Feature | Description | How It Works |
|---------|-------------|--------------|
| **Need & Value Matching** | Connects users based on what they need and what they can offer | Frank evaluates your professional needs (interviews, projects, advice) and matches you with users who can provide value in those areas |
| **Email Context Intelligence** | Understands your life from your inbox | Reads your emails to know you're going to an AI conference, taking CS 101, signed up for a hackathon—then matches you with others doing the same |
| **Location-Aware Networking** | Finds people near you for in-person collaboration | Matches users by university, city, or event location to enable real-world meetups and study sessions |
| **AI-Powered Group Chats** | Creates group chats with context-rich conversation starters | Initiates group conversations with relevant icebreaker polls and questions based on members' shared interests and activities |

## Graphs

Simplified LangGraph-based assistant with four active graphs:
- **Onboarding**: name → school → career interests (Photon reaction on name, contact card sharing)
- **Recommendation**: resource/opportunity recommendations (Azure OpenAI + resources DB + Zep memory)
- **Networking**: match candidates based on need/value/location context and introduce via group chat
- **General**: casual chat with Zep context and fast-path acknowledgements

## Stack
- FastAPI, LangGraph (no checkpoints), Supabase, Azure OpenAI, Zep
- Photon for messaging/typing/reactions
- Stripe helpers retained (not invoked in graphs)

## Run
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill Photon, Supabase, Azure OpenAI, Stripe, Zep
uvicorn app.main:app --reload
```