-- Expand activity_type constraint to include more categories
-- Run this after user_networking_opportunities_v2.sql

-- Drop the existing check constraint
ALTER TABLE user_networking_opportunities
DROP CONSTRAINT IF EXISTS user_networking_opportunities_activity_type_check;

-- Add expanded check constraint with more activity types
ALTER TABLE user_networking_opportunities
ADD CONSTRAINT user_networking_opportunities_activity_type_check
CHECK (activity_type IN (
    'academic',       -- classes, studying, exams, thesis work
    'event',          -- info sessions, career fairs, conferences, workshops
    'project',        -- hackathons, startups, team projects
    'research',       -- research collaborations, lab work, papers
    'social',         -- meals, casual hangouts, networking events
    'hobby',          -- sports, clubs, personal interests
    'activity',       -- recurring activities, gym, sports
    'practice',       -- practice sessions, mock interviews, case studies
    'career',         -- job search, interviews, mentorship
    'collaboration',  -- working together on shared goals
    'mentorship',     -- mentor/mentee relationships
    'networking',     -- general professional networking
    'interview',      -- interview prep, mock interviews
    'meeting',        -- meetings, syncs, 1:1s
    'workshop',       -- workshops, bootcamps, training
    'competition',    -- competitions, contests, challenges
    'general'         -- anything else that doesn't fit above
));
