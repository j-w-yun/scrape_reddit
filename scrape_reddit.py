import csv
import json
import requests
import os
from datetime import datetime


SUBREDDIT = 'wallstreetbets'

URL = 'https://api.pushshift.io/reddit/search/submission'
FILENAME = '{}_data.csv'.format(SUBREDDIT)
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

def get_last_time():
	"""Get latest time from CSV.
	"""
	last_line = ''
	with open(FILENAME, 'r') as f:
		f.seek(0, 2)
		fsize = f.tell()
		f.seek(max (fsize-4096, 0), 0)
		lines = f.read().splitlines()
		last_line = lines[-1]
	return last_line.split(DELIMITER)[0]

def sanitize(s):
	"""Sanitize whitespace and delimiter.
	"""
	res = s
	res = res.replace(DELIMITER, ',')
	res = ' '.join(res.split())
	return res

def save_data(data, write_header=False):
	"""Append data to csv.
	"""
	with open(FILENAME, 'a', encoding='utf-8') as f:
		dw = csv.DictWriter(f, delimiter=DELIMITER, extrasaction='ignore', fieldnames=FIELDNAMES)
		if write_header:
			dw.writeheader()
		for datum in data:
			dw.writerow(datum)

def get_data(start_time=0):
	# Request
	params = {
		'subreddit': SUBREDDIT,
		'size': 500,
		'sort': 'asc',
		'sort_type': 'created_utc',
		'after': start_time,
	}
	res = requests.get(URL, params=params)
	if res.status_code != 200:
		return None

	# Data is a list of dicts
	data = res.json()['data']

	# Sanitize values for csv
	for i in range(len(data)):
		for k in data[i]:
			if isinstance(data[i][k], str):
				data[i][k] = sanitize(data[i][k])
			else:
				dump = json.dumps(data[i][k])
				data[i][k] = json.loads(sanitize(dump))

	# Append data to csv
	save_data(data, write_header=(start_time == 0))

	return data

if __name__ == '__main__':
	# Start from last time in CSV
	last_time = 0;
	if os.path.isfile(FILENAME):
		last_time = get_last_time()

	# Run until CSV is up-to-date
	while True:
		# Get data
		data = get_data(last_time)

		# Data is none if request failed to fetch data
		if data is None:
			continue

		# CSV is up-to-date
		if len(data) == 0:
			print('Up-to-date')
			break

		# Set latest time
		last_time = data[-1]['created_utc']

		print('Fetched {} - {}'.format(
			datetime.fromtimestamp(data[0]['created_utc']),
			datetime.fromtimestamp(last_time)))
