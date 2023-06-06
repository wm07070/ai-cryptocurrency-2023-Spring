import pandas as pd
import os

def cal_mid_price (gr_bid_level, gr_ask_level, group_t, mid_type):
    
    level = 5 
    #gr_rB = gr_bid_level.head(level)
    #gr_rT = gr_ask_level.head(level)
    
    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        mid_price = (bid_top_price + ask_top_price) * 0.5 #what is mid price?
    
        if mid_type == 'wt':
            mid_price = ((gr_bid_level.head(level))['price'].mean() + (gr_ask_level.head(level))['price'].mean()) * 0.5
        elif mid_type == 'mkt':
            mid_price = ((bid_top_price*ask_top_level_qty) + (ask_top_price*bid_top_level_qty))/(bid_top_level_qty+ask_top_level_qty)
            mid_price = round(mid_price, 1)
        elif mid_type == 'vwap':
            mid_price = (group_t['total'].sum())/(group_t['units_traded'].sum())
            # mid_price = truncate(mid_price, 1)
        
        #print mid_type, mid_price

        return (mid_price, bid_top_price, ask_top_price, bid_top_level_qty, ask_top_level_qty)

    else:
        print ('Error: serious cal_mid_price')
        return (-1, -1, -2, -1, -1)
    

def get_sim_df(fn):
    print('loading... %s' % fn)
    df = pd.read_csv(fn).apply(pd.to_numeric, errors='ignore')
    group = df.groupby(['timestamp'])
    return group


raw_directory = 'data_input'  # Raw 데이터가 있는 디렉토리 경로
output_directory = 'output'  # 결과를 저장할 디렉토리 경로
ratio = 0.2
interval = 1
def main():
    csv_files = get_csv_files(raw_directory)  # 디렉토리에서 모든 CSV 파일 경로 가져오기
    
    for csv_file in csv_files:
        process_csv_file(csv_file)

def get_csv_files(directory):
    csv_files = []
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            csv_files.append(os.path.join(directory, file))
    return csv_files

def process_csv_file(csv_file):
    group = get_sim_df(csv_file)  # CSV 파일의 데이터 그룹화
    
    print(f"CSV 파일: {csv_file}")
    print(f"그룹의 개수: {len(group)}")
    
    # 각 그룹의 데이터를 저장할 새로운 CSV 파일 생성
    output_file = os.path.join(output_directory, f"{os.path.splitext(os.path.basename(csv_file))[0]}_output.csv")
    
    with open(output_file, 'w') as f:
        f.write("mid_price,mid_price_wt,mid_price_mkt,book-imbalance-0.2-5-1,total_order_quantity,timestamp\n")  # CSV 파일 헤더 작성
    
    # 각 그룹의 데이터를 CSV 파일에 추가
    for timestamp, data_group in group:
        bid_levels = data_group[data_group['type'] == 0]
        ask_levels = data_group[data_group['type'] == 1]
        
        bid_qty_sum = bid_levels['quantity'] ** ratio
        ask_qty_sum = ask_levels['quantity'] ** ratio
        bid_price_qty_sum = bid_levels['price'] * bid_qty_sum
        ask_price_qty_sum = ask_levels['price'] * ask_qty_sum
        
        bid_qty_sum = bid_qty_sum.sum()
        ask_qty_sum = ask_qty_sum.sum()
        bid_price_qty_sum = bid_price_qty_sum.sum()
        ask_price_qty_sum = ask_price_qty_sum.sum()
        
        book_price = (
            (ask_qty_sum * bid_price_qty_sum) / bid_qty_sum +
            (bid_qty_sum * ask_price_qty_sum) / ask_qty_sum
        ) / (bid_qty_sum + ask_qty_sum)
        
        mid_price = (bid_levels['price'].max() + ask_levels['price'].min()) * 0.5
        mid_price_wt = (bid_levels['price'].mean() + ask_levels['price'].mean()) * 0.5
        mid_price_mkt, _, _, _, _ = cal_mid_price(bid_levels, ask_levels, data_group,'mkt')
        book_imbalance = (book_price - mid_price) / interval
        total_order_quantity = bid_levels['quantity'].sum() + ask_levels['quantity'].sum()
        timestamp_str = str(timestamp).replace("(", "").replace(")", "").replace("'", "").replace(",", "")
    

        # 데이터를 CSV 파일에 추가
        with open(output_file, 'a') as f:
            f.write(f"{mid_price},{mid_price_wt},{mid_price_mkt},{book_imbalance},{total_order_quantity},{timestamp_str}\n")


if __name__ == '__main__':
    main()
