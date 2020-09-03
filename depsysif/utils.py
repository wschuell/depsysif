import datetime



def clean_timestamp(t):
	'''
	Converting timestamp to datetime object if necessary
	'''
	if isinstance(t,str):
		try:
			if len(t) == 10:
				return datetime.datetime.strptime(t, '%Y-%m-%d')
			elif len(t) == 19:
				return datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
			else:
				raise Exception # Just used to trigger the error handling, the specific message is only written once this way
		except:
			raise ValueError('Unknown timestamp format {} : Should be datetime object, or YYYY-MM-DD or YYYY-MM-DD HH:MM:SS'.format(t))
	else:
		return t.replace(microsecond=0)
