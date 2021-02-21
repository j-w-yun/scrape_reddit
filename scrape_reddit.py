import csv
import json
import requests
import os
import os.path
import pandas as pd
import time
from datetime import datetime


URL = 'https://api.pushshift.io/reddit/search/submission'
SUBREDDIT = 'wallstreetbets'
FILENAME = 'wsb_data.csv'
DELIMITER = '|'
FIELDNAMES = [
	'created_utc',
	'all_awardings',
	'allow_live_comments',
	'author',
	'author_cakeday',
	'author_created_utc',
	'author_flair_background_color',
	'author_flair_css_class',
	'author_flair_richtext',
	'author_flair_template_id',
	'author_flair_text',
	'author_flair_text_color',
	'author_flair_type',
	'author_fullname',
	'author_patreon_flair',
	'author_premium',
	'awarders',
	'banned_by',
	# 'brand_safe',
	'can_mod_post',
	'contest_mode',
	'distinguished',
	'domain',
	'edited',
	'full_link',
	'gallery_data',
	'gilded',
	'gildings',
	'id',
	'is_crosspostable',
	'is_gallery',
	'is_meta',
	'is_original_content',
	'is_reddit_media_domain',
	'is_robot_indexable',
	'is_self',
	'is_video',
	'link_flair_background_color',
	'link_flair_css_class',
	'link_flair_richtext',
	'link_flair_template_id',
	'link_flair_text',
	'link_flair_text_color',
	'link_flair_type',
	'locked',
	'media',
	'media_embed',
	'media_metadata',
	'media_only',
	'mod_reports',
	'no_follow',
	'num_comments',
	'num_crossposts',
	'over_18',
	'parent_whitelist_status',
	'permalink',
	'pinned',
	'post_hint',
	'preview',
	'pwls',
	'removed_by_category',
	'retrieved_on',
	'score',
	'secure_media',
	'secure_media_embed',
	'selftext',
	'send_replies',
	'spoiler',
	'stickied',
	'subreddit',
	'subreddit_id',
	'subreddit_subscribers',
	'subreddit_type',
	'suggested_sort',
	'thumbnail',
	'thumbnail_height',
	'thumbnail_width',
	'treatment_tags',
	'title',
	'total_awards_received',
	'upvote_ratio',
	'url',
	'url_overridden_by_dest',
	'user_reports',
	'whitelist_status',
	'wls',
]

def get_last_date():
	with open(FILENAME, 'r') as f:
		last = list(csv.DictReader(f, delimiter=DELIMITER))[-1]
		return int(last['created_utc'])

def sanitize(s):
	res = s
	res = res.replace(DELIMITER, ',')
	res = ' '.join(res.split())
	return res

def get_data(start=0):
	file_exists = False
	if os.path.isfile(FILENAME):
		file_exists = True
		if start == 0:
			start  = get_last_date()

	# Request
	params = {
		'subreddit': SUBREDDIT,
		'size': 500,
		'sort': 'asc',
		'sort_type': 'created_utc',
		'after': start,
	}
	res = requests.get(URL, params=params)
	if res.status_code != 200:
		return None

	data = res.json()['data']
	# print(json.dumps(data, indent=4, sort_keys=True))
	print('Fetched', datetime.fromtimestamp(start))
	for i in range(len(data)):
		for k in data[i]:
			if isinstance(data[i][k], str):
				data[i][k] = sanitize(data[i][k])
			else:
				dump = json.dumps(data[i][k])
				data[i][k] = json.loads(sanitize(dump))

	# Append to csv
	with open(FILENAME, 'a', encoding='utf-8') as f:
		dw = csv.DictWriter(f, delimiter=DELIMITER, extrasaction='ignore', fieldnames=FIELDNAMES)
		# dw = csv.DictWriter(f, delimiter=DELIMITER, fieldnames=FIELDNAMES)
		if not file_exists:
			dw.writeheader()
		for datum in data:
			dw.writerow(datum)

	return data

last_time = 0;
for i in range(10000):
	data = get_data(last_time)
	if data is not None:
		last_time = data[-1]['created_utc']
