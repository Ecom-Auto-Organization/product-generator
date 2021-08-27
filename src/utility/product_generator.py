import re
import pandas as pd
import io
from datamodel.custom_exceptions import MissingArgumentError
from datamodel.custom_enums import FileType
import logging


class ProductGenerator:
    """
    Class to read excel or csv file and generate products from it
    """

    def __init__(self, info):
        if info is not None:
            self._file_obj = info.get('file_object')
            self._file_content = info.get('file_content')
            self._job_type = info.get('job_type')
            self._options = info.get('options')
            self._field_details = self._file_obj['field_details']
        else:
            raise MissingArgumentError('Missing argument for ProductGenerator class')

    

    def get_products(self):
        """
        Method to read excel or csv file and generate products from it
        """

        # RELATIONSHIPS TO TAKE NOTICE
        # LINE = INDEX + 2
        # START_INDEX = HEADER
        # TITLE_INDEX = HEADER - 1
        df = None
        header_row = int(self._file_obj['header_row'])
        start_index_number = header_row
        file_bytes = io.BytesIO(self._file_content)
        file_type = FileType[self._file_obj['file_type']]
        
        if file_type == FileType.EXCEL:
            df = pd.read_excel(file_bytes, header=0)
        elif file_type == FileType.CSV:
            df = pd.read_csv(file_bytes, header=0)
        else:
            raise MissingArgumentError('Couldn\'t process file. File Type must be either CSV or EXCEL file.')

        if 'title' not in self._field_details:
            raise MissingArgumentError('File is missing title column.')
        
        df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
        row_values = df.values.tolist()
        index_values = df.index.values.tolist()
        row_values_start_position = self.__get_first_product_position(index_values, start_index_number)
        
        #if we do not find the position of the first items, it means something is wrong
        if row_values_start_position is None:
            raise Exception('Invalid file. Could not find values start postion. Details... FileId: ' + self._file_obj['id'])
        
        #Get actual row and index values
        row_values = row_values[row_values_start_position:]
        index_values = index_values[row_values_start_position:]

        #if actual row count is not equal to number of row values then something is wrong
        if len(row_values) != len(index_values) and len(row_values) != int(self._file_obj['actual_row_count']):
            raise Exception('Invalid file. Number of row values not equal to actual row count')
        
        products = []

        for current_row in range(len(row_values)):
            # Check if there is a previous row
            product_item = {}
            has_previous_row = False
            if len(products) > 0 and current_row > 0:
                has_previous_row = True

            row_number = index_values[current_row] + 2
            current_row_values = row_values[current_row]
            product_title = self.__get_title(current_row_values)
            product_title = (product_title, 'Invalid Title')[product_title is None]
            prev_product_title = None
            
            if has_previous_row:
                prev_product_title = self.__get_title(row_values[current_row - 1])

            handle = None
            prev_handle = None

            if 'handle' in self._field_details:
                handle = self.__get_handle(current_row_values)

                if has_previous_row:
                    prev_handle = self.__get_handle(row_values[current_row - 1])

            if has_previous_row and (prev_product_title is not None and prev_product_title == product_title) or (prev_handle is not None and prev_handle == handle):
                product_item = products[- 1]
                product_variant = self.__get_product_variant(current_row_values, product_item, row_number)
                if bool(product_variant): product_item['variants'].append(product_variant)
            else:
                product_item['errors'] = []
                product_item['warnings'] = []
                product_item['title'] = product_title
                if handle is not None: product_item['handle'] = handle
                if product_title == 'Invalid Title':
                    error_message = 'Row ' + str(row_number) + ': Product Title is empty'
                    product_item['errors'].append(error_message)

                product_item = self.__get_product_details(current_row_values, product_item, row_number)
                product_item['images'] = []
                product_item['variants'] = []
                product_item['variantTitles'] = list()
                product_variant = self.__get_product_variant(current_row_values, product_item, row_number)
                if bool(product_variant): product_item['variants'].append(product_variant)
                products.append(product_item)
        return products


    def __get_product_details(self, row_values, product_item, row_number):
        """
        Gets and returns product details object with values from the excel or csv file

        Parameters
        ----------
        row_values: list, required
            values from a particular row in the excel or csv file

        product_item: dict, required
            the product item object

        row_number: integer, required
            the current row number within the excel or csv file

        Returns
        ------
        variant: dict
            An object with the product details
        """

        base_output_msg = 'Row ' + str(row_number) + ': '

        if 'descriptionHtml' in self._field_details:
            descriptionHtml = self.__get_description(row_values)
            if len(descriptionHtml) > 0:
                product_item['descriptionHtml'] = descriptionHtml

        if 'vendor' in self._field_details:
            vendor = self.__get_vendor(row_values)
            if vendor is not None:
                product_item['vendor'] = vendor

        if 'productType' in self._field_details:
            product_type = self.__get_product_type(row_values)
            if product_type is not None:
                product_item['productType'] = product_type

        if 'tags' in self._field_details:
            tags = self.__get_tags(row_values)
            if tags is not None:
                product_item['tags'] = tags

        if len(self._options['addedTags']) > 0:
            if 'tags' in product_item:
                product_item['tags'].extend(self._options['addedTags'])
            else:
                product_item['tags'] = self._options['addedTags']

        if 'published' in self._field_details:
            published = self.__get_published(row_values)
            default = self._options['defaultPublishedStatus']
            if published is None:
                product_item['published'] = default
            else:
                if not isinstance(published, bool):
                    warning_message = base_output_msg + 'Invalid published Value. Valid values are: [TRUE, YES, Y] for True, and [FALSE, NO, N] for False. Replacing with default published value.'
                    product_item['warnings'].append(warning_message) 
                    product_item['published'] = default 
                else:
                    product_item['published'] = published
        elif 'published' not in self._field_details:
            default = self._options['defaultPublishedStatus']
            product_item['published'] = default

        if 'option1Value' in self._field_details or 'option2Value' in self._field_details or 'option3Value' in self._field_details:
            product_item['options'] = []

        if 'option1Name' in self._field_details:
            option1_name = self.__get_option1_name(row_values)
            if option1_name is not None:
                product_item['option1Name'] = option1_name
                product_item['options'].append(option1_name)
            else:
                if 'option1Value' in self._field_details:
                    error_message = base_output_msg + 'Value for option1 Name is invalid. Please ensure value is not empty.'
                    product_item['errors'].append(error_message)

        if 'option1Name' not in self._field_details and 'option1Value' in self._field_details:
            option1_name = self.__get_option1_name_from_option_value()
            if option1_name is not None:
                product_item['option1Name'] = option1_name
                product_item['options'].append(option1_name)

        if 'option2Name' in self._field_details:
            option2_name = self.__get_option2_name(row_values)
            if option2_name is not None:
                product_item['option2Name'] = option2_name
                product_item['options'].append(option2_name)
            else:
                if 'option2Value' in self._field_details:
                    error_message = base_output_msg + 'Value for option2 Name is invalid. Please ensure value is not empty.'
                    product_item['errors'].append(error_message)

        if 'option2Name' not in self._field_details and 'option2Value' in self._field_details:
            option2_name = self.__get_option2_name_from_option_value()
            if option2_name is not None:
                product_item['option2Name'] = option2_name
                product_item['options'].append(option2_name)

        if 'option3Name' in self._field_details:
            option3_name = self.__get_option3_name(row_values)
            if option3_name is not None:
                product_item['option3Name'] = option3_name
                product_item['options'].append(option3_name)
            else:
                if 'option3Value' in self._field_details:
                    error_message = base_output_msg + 'Value for option3 Name is invalid. Please ensure value is not empty.'
                    product_item['errors'].append(error_message)

        if 'option3Name' not in self._field_details and 'option3Value' in self._field_details:
            option3_name = self.__get_option3_name_from_option_value()
            if option3_name is not None:
                product_item['option3Name'] = option3_name
                product_item['options'].append(option3_name)

        if 'seoTitle' in self._field_details or 'seoDescription' in self._field_details:
            seo = {}

        if 'seoTitle' in self._field_details:
            seo_title = self.__get_seo_title(row_values)
            if seo_title is not None:
                seo['title'] = seo_title

        if 'seoDescription' in self._field_details:
            seo_description = self.__get_seo_description(row_values)
            if seo_description is not None:
                seo['description'] = seo_description

        if 'seoTitle' in self._field_details or 'seoDescription' in self._field_details:
            product_item['seo'] = seo

        if 'status' in self._field_details:
            status = self.__get_status(row_values)
            default = self._options['defaultStatus']
            if status is None:
                product_item['status'] = default
            else:
                if status == 'INVALID':
                    warning_message = base_output_msg + 'Invalid status Value. Valid values are: ACTIVE, DRAFT, ARCHIVED. Replacing with default status value.'
                    product_item['warnings'].append(warning_message) 
                    product_item['status'] = default
                else:
                    product_item['status'] = status
        elif 'status' not in self._field_details:
            default = self._options['defaultStatus']
            product_item['status'] = default

        if 'customCollections' in self._field_details:
            collections = self.__get_collections(row_values)
            if collections is not None:
                product_item['collectionsToJoin'] = collections

        if 'metafields' in self._field_details:
            metafields = self.__get_metafields(row_values)
            if metafields is not None and len(metafields) > 0:
                product_item['metafields'] = metafields
        
        return product_item


    def __get_product_variant(self, row_values, product_item, row_number):
        """
        Gets and returns product variant details object with values from the excel or csv file

        Parameters
        ----------
        row_values: list, required
            values from a particular row in the excel or csv file

        product_item: dict, required
            the product item object

        row_number: integer, required
            the current row number within the excel or csv file

        Returns
        ------
        variant: dict
            An object with the product vairiant details
        """
        variant = {}
        variant_title = ''
        base_output_msg = 'Row ' + str(row_number) + ': '

        if 'option1Value' in self._field_details or 'option2Value' in self._field_details or 'option3Value' in self._field_details:
            variant['options'] = []
        
        if 'option1Value' in self._field_details:
            if 'option1Name' in product_item:
                option1_value = self.__get_option1_value(row_values)
                if option1_value is not None:
                    variant['options'].append(option1_value)
                    variant_title += option1_value
            else:
                error_message = base_output_msg + 'There is no Option1 Name associated with the Option1 value.'
                product_item['errors'].append(error_message)

        if 'option2Value' in self._field_details:
            if 'option2Name' in product_item:
                option2_value = self.__get_option2_value(row_values)
                if option2_value is not None:
                    variant['options'].append(option2_value)
                    variant_title += '/'
                    variant_title += option2_value
            else:
                error_message = base_output_msg + 'There is no Option2 Name associated with the Option2 value.'
                product_item['errors'].append(error_message)


        if 'option3Value' in self._field_details:
            if 'option3Name' in product_item:
                option3_value = self.__get_option3_value(row_values)
                if option3_value is not None:
                    variant['options'].append(option3_value)
                    variant_title += '/'
                    variant_title += option3_value
            else:
                error_message = base_output_msg + 'There is no Option3 Name associated with the Option3 value.'
                product_item['errors'].append(error_message)

        if variant_title == '': variant_title = 'Default Title'
        if variant_title in product_item['variantTitles']:
            error_message = base_output_msg + 'Variant title ' + variant_title + ', already exist'
            product_item['errors'].append(error_message)
        else:
            product_item['variantTitles'].append(variant_title)

        if 'variantSku' in self._field_details:
            sku = self.__get_variant_sku(row_values)
            if sku is not None:
                variant['sku'] = sku

        if 'variantWeight' in self._field_details:
            weight = self.__get_variant_weight(row_values)
            if weight is not None:
                weightValue = weight['weight']
                if not isinstance(weightValue, float):
                    warning_message = base_output_msg + 'Invalid variant weight value. Value should be a number.'
                    product_item['warnings'].append(warning_message)
                else:
                    variant['weight'] = weight['weight']
                    variant['weightUnit'] = weight['weight_unit']

        if 'variantTracked' in self._field_details or 'variantCost' in self._field_details:
            variant['inventoryItem'] = {}

        if 'variantTracked' in self._field_details:
            tracked = self.__get_variant_tracked(row_values)
            if tracked is not None:
                if not isinstance(tracked, bool):
                    warning_message = base_output_msg + 'Invalid variant tracked Value. Valid values are: [TRUE, YES, Y] for True, and [FALSE, NO, N] for False.'
                    product_item['warnings'].append(warning_message) 
                else:
                    variant['inventoryItem']['tracked'] = tracked

        if 'variantCost' in self._field_details:
            cost = self.__get_variant_cost(row_values)
            if cost is not None:
                if not isinstance(cost, float):
                    warning_message = base_output_msg + 'Invalid variant cost Value. Value should be a number.'
                    product_item['warnings'].append(warning_message)
                else:
                    variant['inventoryItem']['cost'] = cost

        if 'variantQuantity' in self._field_details:
            variant_quantity = self.__get_inventory_quantity(row_values)
            if len(variant_quantity) > 0:
                variant['inventoryQuantities'] = variant_quantity

        if 'variantInventoryPolicy' in self._field_details:
            policy = self.__get_inventory_policy(row_values)
            if policy is not None:
                if policy == 'INVALID':
                    warning_message = base_output_msg + 'Invalid variant policy Value. Valid values are CONTINUE, DENY.'
                    product_item['warnings'].append(warning_message)
                else:
                    variant['inventoryPolicy'] = policy

        if 'variantPrice' in self._field_details:
            price = self.__get_variant_price(row_values)
            if price is not None:
                if not isinstance(price, float):
                    warning_message = base_output_msg + 'Invalid variant price Value. Value should be a number.'
                    product_item['warnings'].append(warning_message)
                else:
                    variant['price'] = price

        if 'variantCompareAtPrice' in self._field_details:
            compare_price = self.__get_compare_price(row_values)
            if compare_price is not None:
                if not isinstance(compare_price, float):
                    warning_message = base_output_msg + 'Invalid variant compate at price Value. Value should be a number.'
                    product_item['warnings'].append(warning_message)
                else:
                    variant['compareAtPrice'] = compare_price

        if 'variantRequireShipping' in self._field_details:
            require_shipping = self.__get_require_shipping(row_values)
            if require_shipping is not None:
                if not isinstance(require_shipping, bool):
                    warning_message = base_output_msg + 'Invalid require shipping Value. Valid values are: [TRUE, YES, Y] for True, and [FALSE, NO, N] for False.'
                    product_item['warnings'].append(warning_message) 
                else:
                    variant['requiresShipping'] = require_shipping

        if 'variantTaxable' in self._field_details:
            taxable = self.__get_variant_taxable(row_values)
            if taxable is not None:
                if not isinstance(require_shipping, bool):
                    warning_message = base_output_msg + 'Invalid variant taxable Value. Valid values are: [TRUE, YES, Y] for True, and [FALSE, NO, N] for False.'
                    product_item['warnings'].append(warning_message) 
                else:
                    variant['taxable'] = taxable

        if 'variantBarcode' in self._field_details:
            barcode = self.__get_barcode(row_values)
            if barcode is not None:
                variant['barcode'] = barcode

        if 'variantTaxcode' in self._field_details:
            taxcode = self.__get_taxcode(row_values)
            if taxcode is not None:
                variant['taxCode'] = taxcode

        if 'imageSrc' in self._field_details:
            images = self.__get_images(row_values)
            if len(images) > 0:
                product_item['images'].extend(images)

        if 'variantImage' in self._field_details:
            image = self.__get_variant_image(row_values)
            if image is not None:
                variant['imageSrc'] = image
                product_item['images'].append({'src': image})
 
        return variant
        
        
    def __is_invalid(self, value):
        if pd.notnull(value) and str(value).strip() is not None and str(value).strip() != '' and value is not None:
            return False
        return True


    def __get_boolean(self, value):
        if self.__is_invalid(value):
            return None
        
        cleaned_value = value.strip().lower()
        if cleaned_value == 'true' or cleaned_value == 'yes' or cleaned_value == 'y':
            return True
        elif cleaned_value == 'false' or cleaned_value == 'no' or cleaned_value == 'n':
            return False
        else:
            return 'INVALID'


    def __get_float(self, value):
        if self.__is_invalid(value):
            return None
        try:
            float_value = float(value)
            return float_value
        except Exception:
            return 'INVALID'


    def __get_int(self, value):
        if self.__is_invalid(value):
            return None
        try:
            int_value = int(value)
            return int_value
        except Exception:
            return 'INVALID'



    def __get_status_value(self, value):
        if self.__is_invalid(value): return None
        cleaned_value = value.strip().lower()
        if len(cleaned_value) <= 7 and 'active' in cleaned_value:
            return 'ACTIVE'
        elif len(cleaned_value) <= 9 and 'archive' in cleaned_value: 
            return 'ARCHIVED'
        elif len(cleaned_value) <= 6 and 'draft' in cleaned_value:
            return 'DRAFT'
        else:
            return 'INVALID'


    def __get_inventory_policy_value(self, value):
        if self.__is_invalid(value): return None
        cleaned_value = value.strip().lower()
        if len(cleaned_value) <= 9 and 'continue' in cleaned_value:
            return 'CONTINUE'
        elif len(cleaned_value) <= 5 and 'deny' in cleaned_value: 
            return 'DENY'
        else:
            return 'INVALID'



    def __get_first_product_position(self, index_list, start_index_number):
        """Gets the row position that we should start reading products from in the spreadsheet

        Parameters
        ----------
        index_list: list, required
            The list of row indexes from pandas or excel sheet

        start_index_number: int, required
            this is the line indicated from the spreadsheet as the first row to read from

        Returns
        ------
        first row to read: int
        """
        for i in range(len(index_list)):
            if index_list[i] == start_index_number:
                return i
        return None

    #----------------------------Methods for getting product details---------------------------

    def __get_title(self, row_values):
        title_index = int(self._field_details['title'][0]['index'])
        product_title = row_values[title_index]
        if self.__is_invalid(product_title):
            return None
        else: 
            return str(product_title)


    def __get_handle(self, row_values):
        handle_index = int(self._field_details['handle'][0]['index'])
        handle = row_values[handle_index]
        if self.__is_invalid(handle):
            return None
        else:
            return str(handle)

    def __get_description(self, row_values):
        descriptionHtml = ''

        description_indexes = self._field_details['descriptionHtml']
        for index in range(len(description_indexes)):
            description_index = int(description_indexes[index]['index'])
            description = row_values[description_index]
            if not self.__is_invalid(description): descriptionHtml += description
            if index < len(description_indexes) - 1: 
                descriptionHtml += '<br>'
        return descriptionHtml

    
    def __get_vendor(self, row_values):
        vendor_index = int(self._field_details['vendor'][0]['index'])
        vendor = row_values[vendor_index]
        if self.__is_invalid(vendor):
            return None
        else:
            return str(vendor)


    def __get_product_type(self, row_values):
        product_type_index = int(self._field_details['productType'][0]['index'])
        product_type = row_values[product_type_index]
        if self.__is_invalid(product_type):
            return None
        else:
            return str(product_type)

    
    def __get_tags(self, row_values):
        """Returns a List of tags"""
        tag_index = int(self._field_details['tags'][0]['index'])
        tags_str = row_values[tag_index]
        if self.__is_invalid(tags_str):
            return None
        else:
            tags_list = re.split(';|,', tags_str)
            return tags_list


    def __get_published(self, row_values):
        published_index = int(self._field_details['published'][0]['index'])
        published_value = row_values[published_index]
        published = self.__get_boolean(published_value)
        return published


    def __get_option1_name(self, row_values):
        option_index = int(self._field_details['option1Name'][0]['index'])
        option = row_values[option_index]
        if self.__is_invalid(option):
            return None
        else:
            return str(option)


    def __get_option1_name_from_option_value(self):
        option_name = self._field_details['option1Value'][0]['defaultOptionName']
        if option_name is None:
            return None
        else:
            return str(option_name)

    def __get_option2_name(self, row_values):
        option_index = int(self._field_details['option2Name'][0]['index'])
        option = row_values[option_index]
        if self.__is_invalid(option):
            return None
        else:
            return str(option)


    def __get_option2_name_from_option_value(self):
        option_name = self._field_details['option2Value'][0]['defaultOptionName']
        if option_name is None:
            return None
        else:
            return str(option_name)


    def __get_option3_name(self, row_values):
        option_index = int(self._field_details['option3Name'][0]['index'])
        option = row_values[option_index]
        if self.__is_invalid(option):
            return None
        else:
            return str(option)


    def __get_option3_name_from_option_value(self):
        option_name = self._field_details['option3Value'][0]['defaultOptionName']
        if option_name is None:
            return None
        else:
            return str(option_name)

    def __get_images(self, row_values):
        images = []
        image_indices = self._field_details['imageSrc']
        for index in range(len(image_indices)):
            image_index = int(image_indices[index]['index'])
            image_sources = row_values[image_index]
            if not self.__is_invalid(image_sources): 
                image_list = re.split(';|,', image_sources)
                for image in image_list:
                    images.append({'src': image})
        return images


    def __get_seo_title(self, row_values):
        seo_title_index = int(self._field_details['seoTitle'][0]['index'])
        seo_title = row_values[seo_title_index]
        if self.__is_invalid(seo_title):
            return None
        else:
            return str(seo_title)


    def __get_seo_description(self, row_values):
        seo_description_index = int(self._field_details['seoDescription'][0]['index'])
        seo_description = row_values[seo_description_index]
        if self.__is_invalid(seo_description):
            return None
        else:
            return str(seo_description)


    def __get_status(self, row_values):
        status_index = int(self._field_details['status'][0]['index'])
        status_value = row_values[status_index]
        status = self.__get_status_value(status_value)
        return status 


    def __get_collections(self, row_values):
        collection_index = int(self._field_details['customCollections'][0]['index'])
        collection_str = row_values[collection_index]
        if self.__is_invalid(collection_str):
            return None
        else:
            collection_list = re.split(';|,', collection_str)
            return collection_list


    def __get_metafields(self, row_values):
        metafields = []
        metafield_indices = self._field_details['metafields']
        for index in range(len(metafield_indices)):
            metafield_index = int(metafield_indices[index]['index'])
            metafield_value = row_values[metafield_index]
            if not self.__is_invalid(metafield_value):
                metafield_name = metafield_indices[index]['name']
                if metafield_name.strip() == '' or metafield_name == None:
                    logging.warning('Missing metafield name in file with id: ' + self._file_obj['id'])
                metafield = {'key': metafield_name, 'value': metafield_value, 'namespace': 'global', 'valueType': 'STRING'}
                metafields.append(metafield)
        return metafields

    #----------------------------Methods for getting variant details---------------------------

    def __get_option1_value(self, row_values):
        option_index = int(self._field_details['option1Value'][0]['index'])
        option = row_values[option_index]
        if self.__is_invalid(option):
            return None
        else:
            return str(option)


    def __get_option2_value(self, row_values):
        option_index = int(self._field_details['option2Value'][0]['index'])
        option = row_values[option_index]
        if self.__is_invalid(option):
            return None
        else:
            return str(option)


    def __get_option3_value(self, row_values):
        option_index = int(self._field_details['option3Value'][0]['index'])
        option = row_values[option_index]
        if self.__is_invalid(option):
            return None
        else:
            return str(option)

    
    def __get_variant_sku(self, row_values):
        sku_index = int(self._field_details['variantSku'][0]['index'])
        sku = row_values[sku_index]
        if self.__is_invalid(sku):
            return None
        else:
            return str(sku)

    
    def __get_variant_weight(self, row_values):
        weight_index = int(self._field_details['variantWeight'][0]['index'])
        weight_value = row_values[weight_index]
        weight = self.__get_float(weight_value)
        if weight is not None:
            weight_unit = self._field_details['variantWeight'][0]['weightUnit']
            return {'weight': weight, 'weight_unit': weight_unit}
        else:
            return None


    def __get_variant_tracked(self, row_values):
        tracked_index = int(self._field_details['variantTracked'][0]['index'])
        tracked_value = row_values[tracked_index]
        tracked = self.__get_boolean(tracked_value)
        return tracked 

    def __get_inventory_quantity(self, row_values):
        inventory = []
        quantity_indices = self._field_details['variantQuantity']
        for index in range(len(quantity_indices)):
            inventory_index = int(quantity_indices[index]['index'])
            quantity_value = row_values[inventory_index]
            quantity = self.__get_int(quantity_value)
            if quantity is not None and isinstance(quantity, int):
                location = self._field_details['variantQuantity'][0]['location']
                if location is None or location == '': 
                    logging.warning('Missing Location for variant Quantity from file with id: ' + self._file_obj['id'])
                quantity = {'availableQuantity': quantity, 'locationId': location}
                inventory.append(quantity)
        return inventory

    
    def __get_inventory_policy(self, row_values):
        policy_index = int(self._field_details['variantInventoryPolicy'][0]['index'])
        policy_value = row_values[policy_index]
        policy = self.__get_inventory_policy_value(policy_value)
        return policy

    
    def __get_variant_price(self, row_values):
        price_index = int(self._field_details['variantPrice'][0]['index'])
        price_value = row_values[price_index]
        price = self.__get_float(price_value)
        return price


    def __get_compare_price(self, row_values):
        compare_price_index = int(self._field_details['variantCompareAtPrice'][0]['index'])
        compare_price_value = row_values[compare_price_index]
        compare_price = self.__get_float(compare_price_value)
        return compare_price


    def __get_require_shipping(self, row_values):
        require_shipping_index = int(self._field_details['variantRequireShipping'][0]['index'])
        require_shipping_value = row_values[require_shipping_index]
        require_shipping = self.__get_boolean(require_shipping_value)
        return require_shipping

    
    def __get_variant_taxable(self, row_values):
        taxable_index = int(self._field_details['variantTaxable'][0]['index'])
        taxable_value = row_values[taxable_index]
        taxable = self.__get_boolean(taxable_value)
        return taxable


    def __get_barcode(self, row_values):
        barcode_index = int(self._field_details['variantBarcode'][0]['index'])
        barcode = row_values[barcode_index]
        if self.__is_invalid(barcode):
            return None
        else:
            return str(barcode)


    def __get_taxcode(self, row_values):
        taxcode_index = int(self._field_details['variantTaxcode'][0]['index'])
        taxcode = row_values[taxcode_index]
        if self.__is_invalid(taxcode):
            return None
        else:
            return str(taxcode)


    def __get_variant_image(self, row_values):
        image_index = int(self._field_details['variantImage'][0]['index'])
        image = row_values[image_index]
        if self.__is_invalid(image):
            return None
        else:
            return str(image)


    def __get_variant_cost(self, row_values):
        cost_index = int(self._field_details['variantCost'][0]['index'])
        cost_value = row_values[cost_index]
        cost = self.__get_float(cost_value)
        return cost






    
          

    





