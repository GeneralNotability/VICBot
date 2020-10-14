'''Ground-up rewrite of VICbot'''

import mwparserfromhell
import MySQLdb
import pywikibot
import pywikibot.pagegenerators
from loguru import logger

TASK_MESSAGE = 'GeneralBotability [[Commons:Bots/Requests/GeneralBotability|task 1]] (maintain VIC):'

error_page_content = ''


def update_random_sample():
    '''
    Update the random sample of valued images.

    Get all pages which transclude {{VI}} in File: and pick four at random.
    '''
    global error_page_content
    # MySQL statement copied from the original vicbot, see https://commons.wikimedia.org/wiki/User:VICbot/source
    try:
        # Doing this via a mysql query is at least an order of magnitude faste rthan doing it through pywikibot
        connection = MySQLdb.connect(host='commonswiki.labsdb', db='commonswiki_p', read_default_file='~/replica.my.cnf')
        cursor = connection.cursor()
        cursor.execute("select page_title from templatelinks, page where tl_title='VI' and tl_namespace=10 and page_namespace=6 and page_id=tl_from order by RAND() limit 4")
        data = cursor.fetchall()
    except MySQLdb.OperationalError as message:
        logger.error('MySQL Error {}'.format(message))
        error_page_content += '* In sample gallery generation: MySQL error\n'
        data = None
    finally:
        cursor.close()
        connection.close()
    if not data:
        return
    sample_gallery_text = '<gallery>'
    for row in data:
        page = pywikibot.Page(pywikibot.getSite(), 'File:{}'.format(row[0].decode()))
        if not page.exists():
            logger.error('Valued image page {} not found'.format(page.title()))
            error_page_content += '* In sample gallery generation: Page {} not found\n'.format(page.as_link())
            continue
        scope = ''
        # extract the scope
        for template in mwparserfromhell.parse(page.text).filter_templates():
            # Looking for the VI template
            if template.name.upper() == 'VI':
                scope = template.get(1).value
                break
        if not scope:
            logger.error('Unable to parse VI template on {}'.format(page.title()))
            error_page_content += '* In sample gallery generation: Failed to parse VI template on {}\n'.format(page.as_link())
            continue

        sample_gallery_text += '[[{}|{}]]\n'.format(page.title(), scope)
    sample_gallery_text += '</gallery>'
    sample_page = pywikibot.Page(pywikibot.getSite(), 'Commons:Valued_images/sample')
    sample_page.text = sample_gallery_text
    sample_page.save(summary=TASK_MESSAGE + ' prepare new random sample of four valued images')


def main():
    pywikibot.handle_args()
    update_random_sample()


if __name__ == '__main__':
    main()
