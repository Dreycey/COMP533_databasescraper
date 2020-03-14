# Import requests, shutil python module.
import requests
import shutil
# This is the image url.
image_url = "http://ids.si.edu/ids/deliveryService?id=NPG-AD-NPG_2011_1"
# Open the url image, set stream to True, this will return the stream content.
resp = requests.get(image_url, stream=True)
# Open a local file with wb ( write binary ) permission.
local_file = open('local_image.jpg', 'wb')
# Set decode_content value to True, otherwise the downloaded image file's size will be zero.
resp.raw.decode_content = True
# Copy the response stream raw data to local image file.
shutil.copyfileobj(resp.raw, local_file)
# Remove the image url response object.

