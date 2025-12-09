import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urlparse

proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890'
}

def handle_age_verification(session, soup, current_url):
    """
    处理年龄认证页面
    """
    if "年齢認証" in soup.get_text() or "age_check" in current_url.lower():
        print("  检测到年龄认证页面，尝试自动处理...")
        
        form = soup.find('form')
        if form:
            action = form.get('action', '')
            method = form.get('method', 'POST').upper()
            
            form_data = {}
            for input_tag in form.find_all(['input', 'select']):
                name = input_tag.get('name')
                if name:
                    if input_tag.get('type') == 'checkbox' and 'age' in name.lower():
                        form_data[name] = '1'
                    elif input_tag.get('value'):
                        form_data[name] = input_tag.get('value')
            
            form_data.update({
                'age_check_done': '1',
                'age_ver': '1',
                'submit': '1'
            })
            
            try:
                if method == 'POST':
                    response = session.post(
                        requests.compat.urljoin(current_url, action), 
                        data=form_data,
                        allow_redirects=True
                    )
                else:
                    response = session.get(
                        requests.compat.urljoin(current_url, action), 
                        params=form_data,
                        allow_redirects=True
                    )
                
                if response.status_code == 200:
                    print("  年龄认证处理成功")
                    return BeautifulSoup(response.text, 'html.parser'), response.url
                
            except Exception as e:
                print(f"  年龄认证处理失败: {e}")
    
    return soup, current_url

def create_session():
    """
    创建带有适当cookie的session
    """
    session = requests.Session()
    
    cookies = {
        'age_check_done': '1',
        'ckcy': '1',
        'age_ver': '1',
        'over18': '1'
    }
    
    for name, value in cookies.items():
        session.cookies.set(name, value, domain='.dmm.co.jp')
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ja,zh-CN;q=0.9,zh;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin'
    })
    
    return session

def extract_search_base(start_url):
    """解析列表URL，返回基础路径、路径后缀、查询字符串和起始页码"""
    parsed = urlparse(start_url)
    path = parsed.path
    marker = '/=/'
    marker_idx = path.find(marker)
    if marker_idx == -1:
        raise ValueError(f"无法解析提供的URL: {start_url}")

    base_prefix = path[:marker_idx + len(marker)]
    remainder = path[marker_idx + len(marker):]

    # 统一处理前后斜杠，确保后缀要么为空要么以'/'结尾
    remainder = remainder.lstrip('/')
    if remainder and not remainder.endswith('/'):
        remainder = f"{remainder}/"

    match = re.match(r'^(.*?)(?:page=(\d+)/)?$', remainder)
    if not match:
        raise ValueError(f"无法解析提供的URL: {start_url}")

    suffix_path = match.group(1)
    start_page = int(match.group(2)) if match.group(2) else 1

    base_path = f"{parsed.scheme}://{parsed.netloc}{base_prefix}"
    query_string = parsed.query
    return base_path, suffix_path, query_string, start_page

def build_page_url(base_path, suffix_path, query_string, page_number):
    """根据页码构建完整URL，适配搜索和标签/厂牌等不同列表"""
    suffix = suffix_path or ''
    path_without_page = f"{base_path}{suffix}"
    query_part = f"?{query_string}" if query_string else ''

    if page_number <= 1:
        return f"{path_without_page}{query_part}"

    if not path_without_page.endswith('/'):
        path_without_page += '/'

    return f"{path_without_page}page={page_number}/{query_part}"

def scrape_dmm_cids(start_url, file_name="cids.txt"):
    """
    抓取所有CID，最后一次性输出和保存
    """
    all_cids = set()  # 存储所有找到的CID
    try:
        base_search_url, suffix_path, query_string, start_page = extract_search_base(start_url)
    except ValueError as e:
        print(str(e))
        return []
    current_page_number = start_page
    processed_pages = 0
    max_pages = 50
    
    # 创建session
    session = create_session()
    session.proxies.update(proxies) if proxies else None
    
    # 先读取已存在的CID
    existing_cids = set()
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            existing_cids = set(line.strip() for line in f if line.strip())
            print(f"已读取文件中的 {len(existing_cids)} 个现有CID")
    except FileNotFoundError:
        print(f"文件 '{file_name}' 不存在，将创建新文件")
    
    print(f"开始抓取所有页面...")
    print("=" * 60)
    
    while processed_pages < max_pages:
        current_url = build_page_url(base_search_url, suffix_path, query_string, current_page_number)
        print(f"正在访问第 {current_page_number} 页...", end=" ")
        
        try:
            response = session.get(current_url, timeout=5)

            if response.history and response.url != current_url:
                print("检测到重定向，说明该页不存在，终止抓取")
                break
            
            if response.status_code != 200:
                print(f"失败 (状态码: {response.status_code})")
                break
            
            if response.encoding.lower() in ['iso-8859-1', 'ascii']:
                response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 处理年龄认证
            soup, current_url = handle_age_verification(session, soup, response.url)
            
            page_text = soup.get_text()
            is_search_page = 'searchstr=' in current_url or '/search/' in current_url

            if is_search_page and "検索結果" not in page_text and "タイトル中" not in page_text:
                clean_url = re.sub(r'/=/', '/', current_url)
                if clean_url != current_url:
                    response = session.get(clean_url, timeout=5)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    current_url = clean_url
            
            # 查找CID链接
            cid_pattern = re.compile(r'/-/detail/=/cid=')
            links = soup.find_all('a', href=cid_pattern)
            
            page_cids = []
            for link in links:
                href = link.get('href', '')
                match = re.search(r'cid=([a-zA-Z0-9_-]+)', href)
                if match:
                    cid = match.group(1)
                    page_cids.append(cid)
                    all_cids.add(cid)
            
            print(f"找到 {len(page_cids)} 个CID (总计: {len(all_cids)})")
            
            # 查找下一页
            processed_pages += 1

            if not page_cids:
                print("页面未找到任何CID，可能已到最后一页")
                break

            if processed_pages >= max_pages:
                print("已达到最大页数限制")
                break

            next_url = build_page_url(base_search_url, suffix_path, query_string, current_page_number + 1)
            print(f"  推断下一页: {next_url}")
            current_page_number += 1
                
        except Exception as e:
            print(f"错误: {e}")
            print("等待5秒后重试...")
            time.sleep(5)
            continue
    
    print("=" * 60)
    print(f"抓取完成！共访问 {processed_pages} 页")
    
    # 计算新CID
    new_cids = all_cids - existing_cids
    
    print(f"\n结果统计:")
    print(f"  原有CID数量: {len(existing_cids)}")
    print(f"  本次找到CID: {len(all_cids)}")
    print(f"  新增CID数量: {len(new_cids)}")
    print(f"  去重后总数: {len(all_cids | existing_cids)}")
    
    # 一次性保存所有CID
    if new_cids:
        print(f"\n新增的CID列表:")
        sorted_new_cids = sorted(new_cids)
        for i, cid in enumerate(sorted_new_cids, 1):
            print(f"  {i:3d}. {cid}")
        
        # 保存到文件 - 追加新CID
        with open(file_name, 'a', encoding='utf-8') as f:
            for cid in sorted_new_cids:
                f.write(cid + '\n')
        print(f"\n已将 {len(new_cids)} 个新CID保存到文件 '{file_name}'")
    else:
        print(f"\n没有发现新的CID")
    
    # 显示所有CID的预览
    all_unique_cids = sorted(all_cids | existing_cids)
    print(f"\n所有CID预览 (前20个):")
    for i, cid in enumerate(all_unique_cids[:20], 1):
        print(f"  {i:3d}. {cid}")
    
    if len(all_unique_cids) > 20:
        print(f"  ... 还有 {len(all_unique_cids) - 20} 个CID")
    
    return list(all_cids)

# 主程序
if __name__ == "__main__":
    print('请输入DMM URL地址：')
    start_url = input().strip()
    
    if not start_url:
        print("URL不能为空")
        exit()
    
    file_name = "cids.txt"
    
    try:
        scrape_dmm_cids(start_url, file_name)
    except KeyboardInterrupt:
        print("\n\n用户中断操作")
    except Exception as e:
        print(f"\n程序异常: {e}")
        import traceback
        traceback.print_exc()