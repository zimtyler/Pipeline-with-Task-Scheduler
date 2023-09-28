import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import io
import sys


XN_DICT = {
	"AccountName": <account_name>,
	"AccountKey": <key>,
	"EndpointSuffix": <end_point>,
	"DefaultEndpointsProtocol": <follow_proto_bro>
}
XN_STR = "".join(s for s in [k + "=" + v + ";" for k, v in XN_DICT.items()])

DESTINATION_LOCATION = (<target_dir>, <sub_dir>)
BS_CLIENT = BlobServiceClient.from_connection_string(XN_STR)


def return_yesterday_and_dateSuffix():
	today_ = datetime.today()
	yesterday = today_ - timedelta(days=1)
	year_month = yesterday.strftime("%Y%m")
	suffix = f"<filename>{year_month}01.csv"

	return yesterday, suffix


def update_cust_dict(suffix):
	cust_dict = {
    "cust0": f"<cust0_subdir/cust0{suffix}"
		"cust1": f"<cust1_subdir/cust1{suffix}",
		"cust2": f"<cust2_subdir/cust2{suffix}",
		"cust3": f"<cust3_subdir/cust3{suffix}",
		"cust4": f"<cust4_subdir/cust4{suffix}"
	}

	return cust_dict


def return_df(client_blob):
	blob_data = client_blob.download_blob()
	# Best to avoid dtype warning, since this table is really a matrix of sorts
	df = pd.read_csv(io.BytesIO(blob_data.readall()), low_memory=False)

	return df


def get_input_blobs(cust_dict, bs_client=BS_CLIENT):
	cust_client_dict = {}

	for cust_container, blob_path in cust_dict.items():
		src_client = bs_client.get_blob_client(cust_container, blob_path)
		cust_client_dict[cust_container] = src_client

	return cust_client_dict


def blob_to_df_tuples(cust_client_dict, yesterday, suffix):
	df_tup_list = []

	for cust, client_blob in cust_client_dict.items():
		df = return_df(client_blob)
		df["latest_data_date"] = yesterday.date()
		filename = f"{cust}_industry_bulletin{suffix}"
		df_tup_list.append((cust, df, filename))

	return df_tup_list


def get_bull_blobs(df_tup_list, bs_client=BS_CLIENT, bull_blob_dir=DESTINATION_LOCATION):
	bull_blob_dict = {}
	try:
		for item in df_tup_list:
			src_client = bs_client.get_blob_client(
				bull_blob_dir[0], bull_blob_dir[1] + item[2])
			bull_blob_dict[item[0]] = src_client

		return bull_blob_dict

	except:
		return False  # TODO Need to update to use actual errors to readjust order of operations --- not super important rn


def bull_blobs_to_df_dict(bull_blob_dict):
	df_bull_dict = {}
	for cust, client_blob in bull_blob_dict.items():
		df = return_df(client_blob)
		df_bull_dict[cust] = df

	return df_bull_dict


def append_dfs(df_tup_list, df_bulletin_dict):
	df_stacked_tups = []
	for item in df_tup_list:
		df_bull = df_bull_dict[item[0]]
		df_bull["latest_data_date"] = pd.to_datetime(
			df_bull["latest_data_date"]).apply(datetime.date)

		most_recent_date = pd.Timestamp(df_bull["latest_data_date"].max())  # validate need for update
		yester_date = pd.Timestamp(item[1]["latest_data_date"].max())

		if most_recent_date < yester_date:
			df_vstack = pd.concat([df_bull, item[1]], ignore_index=True)
			df_stacked_tups.append((df_vstack, item[2]))
		else:
			df_stacked_tups.append(None)
	return df_stacked_tups


def save_to_blobby(df_stacked_tups, bs_client=BS_CLIENT, bull_blob_dir=INDUSTRY_BULLETIN_LOCATION):
	for item in df_stacked_tups:
		if item is not None:
			blob_drop = item[0].to_csv(item[1], index=False)
			blobby_boy = bs_client.get_blob_client(bull_blob_dir[0], bull_blob_dir[1] + item[1])
			with open(item[1], "rb") as blub:
				blobby_boy.upload_blob(blub, overwrite=True)

def main():

    yesterday, suffix = return_yesterday_and_dateSuffix()

    cust_dict = update_cust_dict(suffix)

    cust_blob_clients = get_input_blobs(cust_dict)

    df_tups = blob_to_df_tuples(cust_blob_clients, yesterday, suffix)

    bull_blobs = get_bull_blobs(df_tups)

    bull_blobs_dict = bull_blobs_to_df_dict(bull_blobs)

    appended_dfs_list = append_dfs(df_tups, bull_blobs_dict)

    save_to_blobby(appended_dfs_list)

if __name__ == "__main__":
    main()
