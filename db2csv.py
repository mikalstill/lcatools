#!/usr/bin/python3

import csv
import sys

import mysql.connector
import numpy


conn = mysql.connector.connect(user='root', password=sys.argv[1],
                               host='192.168.1.14', database='rego_prod')
cursor = conn.cursor(dictionary=True, buffered=True)

# Load our data
proposals = {}
cursor.execute('select * from symposion_proposals_proposalbase;')
for row in cursor:
    proposals[row['id']] = row
print('Found %d proposals' % len(proposals))

proposal_types = {}
cursor.execute('select * from symposion_proposals_proposalkind;')
for row in cursor:
    proposal_types[row['id']] = row
print('Found %d proposal types' % len(proposal_types))

reviews = {}
cursor.execute('select * from symposion_reviews_review;')
for row in cursor:
    reviews[row['id']] = row
print('Found %d reviews' % len(reviews))

users = {}
cursor.execute('select * from auth_user;')
for row in cursor:
    users[row['id']] = row
print('Found %d users' % len(users))
print

# Now re-munge all that
for review_id in reviews:
    prop_id = reviews[review_id]['proposal_id']
    user_id = reviews[review_id]['user_id']
    print('Proposal %d, review %d from user %d' %(prop_id, review_id, user_id))
    
    proposals[prop_id].setdefault('reviews', {})
    if not user_id in proposals[prop_id]:
        proposals[prop_id]['reviews'][user_id] = reviews[review_id]
        continue
    
    if (reviews[review_id]['submitted_at'] >
        proposals[prop_id]['reviews'][user_id]['submitted_at']):
        print('... Overwriting old review from user %d' % user_id)
        proposals[prop_id]['reviews'][user_id] = reviews[review_id]

print

# Now output all the proposals with scores
with open('final_scores.csv', 'w') as csvfile:
    cols = ['id',
            'proposal_type',
            'speaker_name',
            'speaker_email',
            'title',
            'submitted',
            'cancelled',
            'status',
            'score',
            'total_votes',
            'minus_two',
            'minus_one',
            'plus_one',
            'plus_two']
    writer = csv.DictWriter(csvfile, fieldnames=cols)
    writer.writeheader()

    for prop_id in proposals:
        proposal = proposals[prop_id]
        speaker = users[proposals[prop_id]['speaker_id']]

        scores = []
        buckets = {}
        for reviewer_id in proposal['reviews']:
            review = proposal['reviews'][reviewer_id]
            buckets.setdefault(review['vote'], 0)
            buckets[review['vote']] += 1

            if review['vote'] == '0':
                continue
            scores.append(int(review['vote']))
        
        writer.writerow(
            {'id': prop_id,
             'proposal_type': proposal_types[proposal['kind_id']]['name'],
             'speaker_name': '%s %s' %(speaker['first_name'],
                                       speaker['last_name']),
             'speaker_email': speaker['email'],
             'title': proposal['title'],
             'submitted': proposal['submitted'],
             'cancelled': {1: 'Yes', 0: 'No'}[proposal['cancelled']],
             'status': 'None',
             'score': numpy.average(scores),
             'total_votes': len(scores),
             'minus_two': buckets.get('-2', 0),
             'minus_one': buckets.get('-1', 0),
             'plus_one': buckets.get('+1', 0),
             'plus_two': buckets.get('+2', 0),
             })
