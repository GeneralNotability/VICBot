'''Ground-up rewrite of VICbot.'''
import re

import mwparserfromhell
import MySQLdb
import pywikibot
import pywikibot.pagegenerators
import pywikibot.textlib
from loguru import logger

TASK_MESSAGE = 'VICBot2 [[Commons:Bots/Requests/VICBot2|task 1]] (maintain VIC):'
USER_PARSER_RE = re.compile(r'\[\[User:(.*?)(?:\|.*)?\]\]', re.I)
CANDIDATE_INPUT_PAGES =  ['Commons:Valued image candidates/candidate list',
                          'Commons:Valued image candidates/Most valued review candidate list']
error_page_content = ''


def update_random_sample():
    '''
    Update the random sample of valued images.

    Get all pages which transclude {{VI}} in File: and pick four at random.
    '''
    global error_page_content
    logger.info('Updating random VI sample page')
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
    sample_gallery_text = '<gallery>\n'
    for row in data:
        page = pywikibot.Page(pywikibot.Site(), 'File:{}'.format(row[0].decode()))
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

        sample_gallery_text += '{}|{}\n'.format(page.title(), scope)
    sample_gallery_text += '</gallery>'
    sample_page = pywikibot.Page(pywikibot.Site(), 'Commons:Valued_images/sample')
    sample_page.text = sample_gallery_text
    sample_page.save(summary=TASK_MESSAGE + ' prepare new random sample of four valued images')


def find_candidate_list():
    '''
    Get all candidates listed as ready for promotion.
    '''
    candidate_list = set()
    for page_title in CANDIDATE_INPUT_PAGES:
        candidate_input_page = pywikibot.Page(pywikibot.Site(), page_title)
        for template in mwparserfromhell.parse(candidate_input_page.text).filter_templates():
            # hacky fix - sometimes there's a byte order mark hiding in the template name,
            # that breaks string comparison
            if re.sub('\u200e', '', str(template.name)).strip() == 'VICs':
                for entry in template.params:
                    # Strip out any HTML comments and normalize on no-underscores for
                    candidate_list.add(re.sub('<!--.*-->', '', str(entry), flags=re.S).strip().replace('_', ' '))

    return list(candidate_list)


def find_promotion_ready(candidate_list):
    '''
    Generates a list of images which can be promoted to VI.

    candidate_list: list of images which are listed on the VIC candidate pages
    '''
    global error_page_content

    ready_to_promote = []
    failed_promotion = []
    for candidate in candidate_list:
        vic_page = pywikibot.Page(pywikibot.Site(), 'Commons:Valued image candidates/{}'.format(candidate))
        if not vic_page.exists():
            logger.warning('VIC page for {} missing'.format(candidate))
            error_page_content += '* In candidate evaluation for [[{}]]: VIC page missing\n'.format(candidate)
            continue
        status = ''
        for cat in vic_page.categories():
            if 'valued image candidates' in cat.title():
                status = cat.title().split(' ')[0].split(':')[1]
        # These categories aren't ready for action, skip
        if not status:
            logger.debug('Candidate {} does not have a VIC discussion category, no action needed'.format(candidate))
            continue
        elif status in ['Nominated', 'Discussed', 'Supported', 'Opposed']:
            logger.debug('Candidate {} has VIC discussion status {}, no action needed'.format(candidate, status))
            continue
        elif status in ['Declined', 'Undecided', 'Withdrawn']:
            failed_promotion.append(candidate)
            continue
        # This should only be stuff approved to promote
        vic_page_text = vic_page.text
        # normalize - the noinclude and includeonly tags mess up mwparserfromhell
        vic_page_text = re.sub(r'<\/?noinclude>', '', vic_page_text, flags=re.I)
        # there are two template starts between the noinclude/includeonly, so remove one
        vic_page_text = vic_page_text.replace('<includeonly>{{VIC-thumb</includeonly>', '')
        entry = {}
        entry['subpage'] = ''
        entry['scope'] = ''
        entry['username'] = ''
        entry['image'] = ''
        entry['review'] = ''
        nominator = ''
        for template in mwparserfromhell.parse(vic_page_text).filter_templates():
            # Looking for the VI template
            if template.name.matches('VIC'):
                try:
                    # Explicit cast to str because these are all "wikicode" objects
                    entry['scope'] = str(template.get('scope').value).strip()
                    nominator = str(template.get('nominator').value).strip()
                    entry['image'] = str(template.get('image').value).strip().replace('_', ' ')
                except ValueError:
                    # handled below
                    pass

                # These are their own sections because we want to try to resolve each
                # even if the other failed
                if template.has('subpage'):
                    entry['subpage'] = str(template.get('subpage').value).strip()
                if template.has('review'):
                    entry['review'] = str(template.get('review').value).strip()
                break
        if not entry['scope'] or not nominator or not entry['image']:
            # Can't finish the promotion
            logger.warning('Critical params missing from nomination for {}'.format(candidate))
            error_page_content += '* In candidate evaluation for [[{}]]: missing critical parameters on nomination page\n'.format(candidate)
            continue
        if not entry['subpage']:
            entry['subpage'] = entry['image']
        try:
            entry['username'] = USER_PARSER_RE.search(nominator).group(1)
        except:
            logger.warning('Unable to parse username from {}'.format(nominator))
            error_page_content += '* In candidate evaluation for [[{}]]: unable to parse username\n'.format(candidate)
            continue
        ready_to_promote.append(entry)
    return ready_to_promote, failed_promotion


def promote_candidates(ready_list):
    user_notifications = {}
    for entry in ready_list:
        # Mark the image as promoted
        logger.info('Promoting File:{}'.format(entry['image']))
        image_page = pywikibot.Page(pywikibot.Site(), 'File:{}'.format(entry['image']))
        # Issue 6: If the image page is a redirect, resolve the redirect (we can't edit the redirect page, it will fail)
        if image_page.isRedirectPage():
            image_page = image_page.getRedirectTarget()
        image_page.text += '\n{{{{subst:VI-add|{}|subpage={}}}}}'.format(entry['scope'], entry['subpage'])
        image_page.save(summary='{} promoting image to Valued Image'.format(TASK_MESSAGE))

        # Add to the to-notify list
        notification = '{{{{VICpromoted|{}|{}|review={}|subpage={}}}}}'.format(entry['image'], entry['scope'], entry['review'], entry['subpage'])
        if entry['username'] in user_notifications:
            user_notifications[entry['username']] += '\n{}'.format(notification)
        else:
            user_notifications[entry['username']] = '{}'.format(notification)

    for user in user_notifications:
        logger.info('Notifying User:{}'.format(user))
        user_talk_page = pywikibot.Page(pywikibot.Site(), 'User talk:{}'.format(user))
        text = ''
        if user_talk_page.exists():
            text = user_talk_page.text + '\n'
        text = text + '==Valued Image Promoted==\n{}\n--~~~~'.format(user_notifications[user])
        user_talk_page.text = text
        user_talk_page.save(summary='{} notify user of promoted VI(s)'.format(TASK_MESSAGE), minor=False)


def remove_candidates(candidates_to_remove):
    logger.info('Removing promoted and failed candidates')
    text 'Commons:Valued image candidates/candidate list',
                          'Commons:Valued image candidates/Most valued review candidate list']
    for page_title in CANDIDATE_INPUT_PAGES:
        candidate_input_page = pywikibot.Page(pywikibot.Site(), page_title)
        parsed = mwparserfromhell.parse(text)
        for template in parsed.filter_templates():
            if not template.name.matches('VICs'):
                continue
            for param in template.params:
                if param.value.matches(candidate):
                    if 'Most valued review candidate list' in page_title:
                        # If we're closing out a review, delete the whole thing
                        parsed.remove(template)
                    else:
                        template.params.delete(param)
        candidate_input_page.text = parsed
        candidate_input_page.save(summary='{} remove promoted and failed VICs'.format(TASK_MESSAGE))


def add_recently_promoted(ready_list):
    new_entries = ''
    for entry in ready_list:
        new_entries += 'File:{}|{}\n'.format(entry['image'], entry['scope'])
    recently_promoted_page = pywikibot.Page(pywikibot.Site(), 'Commons:Valued images/Recently promoted')
    recently_promoted_page.text = recently_promoted_page.text.replace('</gallery>', '{}</gallery>'.format(new_entries))
    recently_promoted_page.save(summary='{} add recently promoted images'.format(TASK_MESSAGE))


def move_sorted_recently_promoted():
    recently_promoted_page = pywikibot.Page(pywikibot.Site(), 'Commons:Valued images/Recently promoted')
    text = recently_promoted_page.text
    for line in text.split('\n'):
        for template in mwparserfromhell.parse(line).filter_templates():
            if template.name == 'VICbotMove':
                image = line.split('|')[0]
                scope = template.get(1)
                topic = template.get(2)
                text = text.replace(line + '\n', '')
                target_page = pywikibot.Page(pywikibot.Site(), 'Commons:Valued images by topic/{}'.format(topic))
                target_page.text = target_page.text.replace('</gallery>', '{}|{}\n</gallery>'.format(image, scope))
                target_page.save(summary='{} add sorted image'.format(TASK_MESSAGE))
    recently_promoted_page.text = text
    recently_promoted_page.save(summary='{} remove sorted images'.format(TASK_MESSAGE))


def write_error_page():
    error_page = pywikibot.Page(pywikibot.Site(), 'User:VICBot2/errors')
    error_page.text = error_page_content
    error_page.save(summary='{} report errors'.format(TASK_MESSAGE))


def main():
    pywikibot.handle_args()
    update_random_sample()
    candidate_list = find_candidate_list()
    ready_list, failed_list = find_promotion_ready(candidate_list)
    promote_candidates(ready_list)
    add_recently_promoted(ready_list)
    move_sorted_recently_promoted()
    remove_candidates(failed_list + [x['image'] for x in ready_list])
    write_error_page()


if __name__ == '__main__':
    main()
