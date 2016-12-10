# -*- coding: utf-8 -*-
"""
Created on Sun Nov 20 16:22:11 2016

@author: Shruti
"""

from apiclient.discovery import build #pip install google-api-python-client
from apiclient.errors import HttpError #pip install google-api-python-client
from oauth2client.tools import argparser #pip install oauth2client
import pandas as pd #pip install pandas
import argparse
import numpy as np
import os
import json


class CreateRawData:
    def buildData(self, genres):
        
        DEVELOPER_KEY = "AIzaSyC9T11pRfgZSFHpM_Dwh1Tub9JaoroYuH8" 
        YOUTUBE_API_SERVICE_NAME = "youtube"
        YOUTUBE_API_VERSION = "v3"
        
        argparser = argparse.ArgumentParser(prog='PROG', conflict_handler='resolve')
        
        argparser.add_argument("--q", help="Search term", default=genres)
        #change the default to the search term you want to search
        argparser.add_argument("--max-results", help="Max results", default=50)
        #default number of results which are returned. It can vary from 0 - 100
        args = argparser.parse_args()
        options = args
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)
        
        # Call the search.list method to retrieve results matching the specified
        # query term.
        search_response = youtube.search().list(
         q=options.q,
         type="video",
         part="id,snippet",
         maxResults=options.max_results
        ).execute()
        
        
        
        videos = {}
        
        # Add each result to the appropriate list, and then display the lists of
        # matching videos.
        # Filter out channels, and playlists.
        for search_result in search_response.get("items", []):
         if search_result["id"]["kind"] == "youtube#video":
        #     videos.append("%s" % (search_result["id"]["videoId"]))
             videos[search_result["id"]["videoId"]] = search_result["snippet"]["title"]
         
        # print "Videos:\n", "\n".join(videos), "\n"
         
        s = ','.join(videos.keys())
        
        videos_list_response = youtube.videos().list(
         id=s,
         part='id,statistics'
        ).execute()
        
#        v_id = []
        v_title = []
        view_count = []
        like_count = []
        dislike_count = []
        comment_count = []
        
        
        for i in videos_list_response['items']:
#            v_id.append(i['id'])
            v_title.append( videos[i['id']])
            if 'viewCount' in i['statistics']:
                view_count.append(i['statistics']['viewCount'])
            else:
                view_count.append(int(0))
            if 'commentCount' in i['statistics']:                
                comment_count.append(i['statistics']['commentCount'])
            else:
                comment_count.append(int(0))
            if 'likeCount' in i['statistics']:
                like_count.append(i['statistics']['likeCount'])
            else:
                like_count.append(0)
            if 'dislikeCount' in i['statistics']:    
                dislike_count.append(i['statistics']['dislikeCount'])
            else:
                dislike_count.append(int(0))
                
        # create required dataframe
        videoDetail = pd.DataFrame(
            {'v_title': v_title,
             'view_count': view_count,
             'like_count': like_count,
            'dislike_count': dislike_count,
            'comment_count': comment_count
            }, columns=['category','genre','v_title','view_count', 'like_count', 'dislike_count', 'comment_count'])
        
        return videoDetail

    def YoutubeDF(self):
        
        hollywood = self.buildData("Hollywood Action Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Action Movie Trailer")
        bollywood['category'] = 'Bollywood'
        action = hollywood.append(bollywood)
        action['genre'] = 'Action'
        
           
        hollywood = self.buildData("Hollywood Family Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Family Movie Trailer")
        bollywood['category'] = 'Bollywood'
        family = hollywood.append(bollywood)
        family['genre'] = 'Family'
        
        
        hollywood = self.buildData("Hollywood Fantasy Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Fantasy Movie Trailer")
        bollywood['category'] = 'Bollywood'
        fantasy = hollywood.append(bollywood)
        fantasy['genre'] = 'Fantasy'
         
        
        hollywood = self.buildData("Hollywood Crime Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Crime Movie Trailer")
        bollywood['category'] = 'Bollywood'
        crime = hollywood.append(bollywood)
        crime['genre'] = 'Crime'
         
        
        hollywood = self.buildData("Hollywood Horror Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Horror Movie Trailer")
        bollywood['category'] = 'Bollywood'
        horror = hollywood.append(bollywood)
        horror['genre'] = 'Horror'
        
        
        hollywood = self.buildData("Hollywood Romantic Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Romantic Movie Trailer")
        bollywood['category'] = 'Bollywood'
        romantic = hollywood.append(bollywood)
        romantic['genre'] = 'Romantic'
        
        
        hollywood = self.buildData("Hollywood Thriller Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Thriler Movie Trailer")
        bollywood['category'] = 'Bollywood'
        thriller = hollywood.append(bollywood)
        thriller['genre'] = 'Thriller'
        
        
        hollywood = self.buildData("Hollywood Comedy Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Comedy Movie Trailer")
        bollywood['category'] = 'Bollywood'
        comedy = hollywood.append(bollywood)
        comedy['genre'] = 'Comedy'
        
        
        hollywood = self.buildData("Hollywood Drama Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Drama Movie Trailer")
        bollywood['category'] = 'Bollywood'
        drama = hollywood.append(bollywood)
        drama['genre'] = 'Drama'
         
        
        hollywood = self.buildData("Hollywood Animation Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Animation Movie Trailer")
        bollywood['category'] = 'Bollywood'
        animation = hollywood.append(bollywood)
        animation['genre'] = 'Animation'
     
        
        hollywood = self.buildData("Hollywood Fiction Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Fiction Movie Trailer")
        bollywood['category'] = 'Bollywood'
        fiction = hollywood.append(bollywood)
        fiction['genre'] = 'Fiction'

        hollywood = self.buildData("Hollywood War Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood War Movie Trailer")
        bollywood['category'] = 'Bollywood'
        war = hollywood.append(bollywood)
        war['genre'] = 'War'
        
        hollywood = self.buildData("Hollywood Mystery Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Mystery Movie Trailer")
        bollywood['category'] = 'Bollywood'
        mystery = hollywood.append(bollywood)
        mystery['genre'] = 'Mystery'
        
        hollywood = self.buildData("Hollywood Adventure Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Adventure Movie Trailer")
        bollywood['category'] = 'Bollywood'
        adventure = hollywood.append(bollywood)
        adventure['genre'] = 'Adventure'
        
        hollywood = self.buildData("Hollywood History Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood History Movie Trailer")
        bollywood['category'] = 'Bollywood'
        history = hollywood.append(bollywood)
        history['genre'] = 'History'
        
        hollywood = self.buildData("Hollywood Documentary Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Documentary Movie Trailer")
        bollywood['category'] = 'Bollywood'
        documentary = hollywood.append(bollywood)
        documentary['genre'] = 'Documentary'
        
        hollywood = self.buildData("Hollywood Sci-Fi Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Sci-Fi Movie Trailer")
        bollywood['category'] = 'Bollywood'
        sciFi = hollywood.append(bollywood)
        sciFi['genre'] = 'Sci-Fi'
        
        hollywood = self.buildData("Hollywood Biography Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Biography Movie Trailer")
        bollywood['category'] = 'Bollywood'
        biography = hollywood.append(bollywood)
        biography['genre'] = 'Biography'
        
        hollywood = self.buildData("Hollywood Musical Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Musical Movie Trailer")
        bollywood['category'] = 'Bollywood'
        musical = hollywood.append(bollywood)
        musical['genre'] = 'Musical'
        
        hollywood = self.buildData("Hollywood Film-Noir Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Film-Noir Movie Trailer")
        bollywood['category'] = 'Bollywood'
        filmNoir = hollywood.append(bollywood)
        filmNoir['genre'] = 'Film-Noir'
        
        hollywood = self.buildData("Hollywood Sports Movie Trailer")
        hollywood['category'] = 'Hollywood'
        bollywood = self.buildData("Bollywood Sports Movie Trailer")
        bollywood['category'] = 'Bollywood'
        sports = hollywood.append(bollywood)
        sports['genre'] = 'Sports'
        
        genres = [action, animation, comedy, crime, drama, family, fantasy, fiction, horror, romantic, thriller,history,war,documentary,biography,sports,adventure,sciFi,mystery,musical,filmNoir]
        dataset = pd.concat(genres)
        
        return dataset
    

if __name__ == '__main__':
    
    obj = CreateRawData()
    
# query youtube for hollywood and bollywood movie trailer per genre
    df = obj.YoutubeDF()
    
# create json per row of df 
    complete_name = os.path.join( '/home/shruti/','rawdata.txt') # write the average to a local file directory
    with open(complete_name, "w") as f:    
        for i in df.index:
            data = df.loc[i].to_json()
            print(data)
            f.write(data+"\n")
        f.close()        

    print 'done'
    
    
    
    


        
