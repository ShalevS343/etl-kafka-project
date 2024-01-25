# def send_to_producer(response):
#     # send response to producer
#     #print(json.dumps(response, indent=2))
#     [update_json_file('response_data.json', {response['results'][i]['id']: response['results'][i]}) for i in range(len(response['results']))]
#     print('New Movie Added!')


# def rapid_api_gather(title, year):
#     # rapid api gather to gather data for the movies
#     url = f"https://moviesdatabase.p.rapidapi.com/titles/search/title/{title}"

#     querystring = {"titleType":"movie", 'year': year}


#     response = requests.get(url, headers=RAPID_HEADERS, params=querystring)

#     return send_to_producer(response.json())

