class getFilterdetails:

    def getmainFilterDetails(self, data_filter, filter_details):

        group = []
        for i in  data_filter['group_by']['columns']:
            if i in filter_details:
                group.append(filter_details[i])
        return group


