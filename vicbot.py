#!/usr/bin/python

import os
import re
import sys

import pywikibot
import mwparserfromhell
import MySQLdb
import viutil # @User:VICbot/source/viutil.py

from loguru import logger
    
# Pre-compiled REs
link2RE = re.compile('\[\[(?:[^\|\]]+\|){0,1}([^\|\]]+)\]\]')
link3RE = re.compile('\{\{\w\|([^\|\}]+)\}\}')
quotesRE = re.compile("'{2,3}")

userRE = re.compile('\[\[([Uu]ser|[Bb]enutzer|[Gg]ebruiker):([^\|\]]+)[^\]]*\]\]')
linkRE = re.compile('\[\[([^\|\]:]+)[^\]]*\]\]')
emptyRE = re.compile('^===[^=]+===\s+^{\{VICs\s+^\}\}\s*', re.MULTILINE)
#galleryRE = re.compile('^\s*[Ii]mage:([^\|]+)')
galleryRE = re.compile('^\s*([Ii]mage|[Ff]ile):([^\|]+)')
viscopeRE = re.compile('^\{\{[vV]I\|(.+)\|[^\|]+\|')
scopelistRE = re.compile('\*\s*\[\[:[Ii]mage:([^\|\]]+).*\|(.+)\]\]\s*$')

TASK_MESSAGE = 'GeneralBotability [[Commons:Bots/Requests/GeneralBotability|task 1]] (maintain VIC): '
ERROR_PAGE = 'User:GeneralBotability/Task 1 errors'

class VICbot:

  def __init__(self):
    """Constructor."""
    self.site = pywikibot.getSite()
    self.error_page_content = ''

  def scrubscope(self, t) :
    t2 = link2RE.sub(r'\1', t )
    t3 = link3RE.sub(r'\1', t2 )
    t4 = quotesRE.sub('', t3)
    return t4

  def run(self):
    userNote = {}
    removeCandidates = []
    tagImages = []
    scopeList = []
    numChanges = 0


    pageName = 'Commons:Valued_image_candidates'
    
    #
    # prepare a random sample for COM:VI
    #

    try:
      connection = MySQLdb.connect(host="commonswiki.labsdb", db="commonswiki_p", read_default_file="~/replica.my.cnf" )
      cursor = connection.cursor() 
      cursor.execute( "select page_title from templatelinks, page where tl_title='VI' and tl_namespace=10 and page_namespace=6 and page_id=tl_from order by RAND() limit 4" )
    except MySQLdb.OperationalError as message:
      logger.error('MySQL Error {}: {}'.format(message[0], message[1]))
      self.error_page_content += '* In sample gallery generation: MySQL error\n'
      self.save_error_page()
      return
    data = cursor.fetchall()
    cursor.close()
    connection.close()

    sample = "<gallery>\n"

    for row in data:
      name = row[0]
      scope = ''

      page = pywikibot.Page(self.site, 'File:{}'.format(name.decode()))
      text = ""
      if page.exists():
        text = page.get(get_redirect=True)

        for line in text.split('\n'):

          # find first scope
          scopematch = viscopeRE.search( line ) 
          if scopematch != None :
            scope = scopematch.group(1)
            continue

      else:
        logger.warning('Odd, VI image page ({}) does not exist!'.format(name))
        self.error_page_content += '* In sample gallery generation: VI page {} does not exist\n'.format(name)
        continue

      sample += 'File:{}|{}\n'.format(name, scope)

    sample += "</gallery>"

    page = pywikibot.Page(self.site, 'Commons:Valued_images/sample' )
    page.text = sample
    logger.trace('Gallery:\n{}'.format(page.text))
    page.save(summary='{} prepare new random sample of four valued images'.format(TASK_MESSAGE))

    #
    # now fetch potential candidate pages
    #
    
    try:
      connection = MySQLdb.connect(host="commonswiki.labsdb", db="commonswiki_p", read_default_file="~/replica.my.cnf" )
      cursor = connection.cursor() 
      cursor.execute( "select /* SLOW_OK */ page_title, GROUP_CONCAT( DISTINCT cl_to SEPARATOR '|') from revision, page left join categorylinks on page_id = cl_from  where page_latest=rev_id and page_title like 'Valued_image_candidates/%' and page_namespace=4 and ( TO_DAYS(CURRENT_DATE) - TO_DAYS(rev_timestamp) ) < 25 group by page_id" )
    except MySQLdb.OperationalError as message: 
      logger.error('MySQL Error {}: {}'.format(message[0], message[1]))
      self.error_page_content += '* In candidate evaluation: MySQL error\n'
    else:
      data = cursor.fetchall() 
      cursor.close()
      connection.close()
      
    candpages = [ "/candidate_list", "/Most valued review candidate list" ]

    candidates = ''
    for candpage in candpages :
      page = pywikibot.Page(self.site, pageName + candpage )
      text = viutil.unescape( page.get(get_redirect=True) )

      # abort if the qicbot marker is missing from the page 
      if text.find("<!-- VICBOT_ON -->") < 0 :
        logger.debug("the string <!-- VICBOT_ON --> was not found on page " + pageName + candpage)
      else :
        candidates += text

    #
    # get potential candidates from db
    #
    
    for row in data:
      # Stored as bytes-like, need to decode
      name = row[0].decode()
      if not row[1]:
        logger.debug('Candidate {} has no categories!'.format(name))
        continue
      cats = row[1].decode()

      catlist = cats.split('|')

      status = 0

      if 'Supported_valued_image_candidates' in catlist:
        status = 0
      if 'Opposed_valued_image_candidates' in catlist:
        status = 0
      if 'Promoted_valued_image_candidates' in catlist:
        status = 1
      if 'Undecided_valued_image_candidates' in catlist:
        status = -1
      if 'Declined_valued_image_candidates' in catlist:
        status = -1
      if 'Discussed_valued_image_candidates' in catlist:
        status = 0
      if 'Nominated_valued_image_candidates' in catlist:
        status = 0

      if status == 0 :
        logger.debug('Nothing to do here ({}, {})'.format(name, cats))
        continue

      #
      # get nomination subpage
      #
    
      page = pywikibot.Page(self.site, 'Commons:{}'.format(name))
      text = ''
      if page.exists() :
        text = page.get(get_redirect=True)
      else :
        logger.warning('Odd, VIC subpage does not exist!')
        self.error_page_content += '* In candidate evaluation for [[{}]]: VIC subpage missing\n'.format(name)
        continue

      #
      # extract parameters TODO: use mwparserfromhell
      #
    
      subpage = ''
      image   = ''
      scope   = ''
      nominator = ''
      review = ''
      recordingReview = False
    
      for rawline in text.split('\n') :
        line = rawline.lstrip(' ')

        if line[:9] == '|subpage=' and subpage == '' :
          subpage = viutil.unescape( line[9:] ).lstrip(' ')
        if line[:7] == '|image=' and image == '' :
          image = line[7:]
        if line[:7] == '|scope=' and scope == '' :
          scope = line[7:]
        if line[:11] == '|nominator=' and nominator == '' :
          user = userRE.search(line)
          if user is not None :
            nominator = user.group(2)

        if line[:8] == '|review=' :
          recordingReview = True
        if recordingReview :
          review += rawline + "\n"

      if image == '' or scope == '' or nominator == '' :
        if image == '' :
          logger.debug('image missing')
        if scope == '' :
          logger.debug('scope missing')
        if nominator == '' :
          logger.debug('nominator missing')
        logger.warning('Candidate {} is missing crucial parameters'.format(name))
        self.error_page_content += '* In candidate evaluation for [[{}]]: review missing parameters\n'.format(name)
        continue
  
      if subpage == '' :
        subpage = image

      if candidates.replace( ' ', '_' ).find(subpage.replace( ' ', '_' ) ) < 0 :
        logger.debug('Candidate {} is not listed, I assume it was already handled!'.format(subpage))
        continue

      if not '}}' in review:
        logger.warning('Unable to extract the review from {}'.format(name))
        self.error_page_content += '* In candidate evaluation for [[{}]]: failed to parse review\n'.format(name)
        review = '}}}}'

      logger.info('Handling ({}) {} on {}, nominated by {}'.format( status, image, subpage, nominator ))
      numChanges += 1

      # queue for removal from candidate list
      removeCandidates.append(subpage)

      if status == 1:

        #spParam = ''
        #if subpage != image :
        spParam = '|subpage=' + subpage
    
        # queue user notification
        # Double '{{' so that the format string interprets them as literal braces
        notification = '{{{{VICpromoted|{}|{}{}{}\n'.format( image, scope, spParam, review)
        if nominator in userNote:
          userNote[nominator] += notification
        else:
          userNote[nominator] = notification

        # queue image page tagging
        tagImages.append([image, '{{{{subst:VI-add|{}{}}}}}\n'.format(scope, spParam), 'File:{}|{}'.format(image, scope)])

        # queue for insertion into alphabetical scope list
        scopeList.append( [ image, scope ] )


    #
    # Alphabetical scope list (this is alway executed as the list might have been edited manually)
    #
    
    page = pywikibot.Page(self.site, 'Commons:Valued_images_by_scope' )
    if page.exists() :
      text = page.get(get_redirect=True)
      oldtext = text
      newList = {}

      for entry in scopeList :
        scrubbed = self.scrubscope(entry[1])
        newList[scrubbed.replace("'",'').upper()] = "*[[:File:{}|{}]]".format(entry[0], scrubbed)

      for line in text.split('\n') :
        match = scopelistRE.search(line)
        if match:
          newList[ match.group(2).replace("'","").upper() ] = line

      sortedList = "\n".join( map(newList.get, sorted(newList)))

      listPrinted = False
      newText = ''
      for line in text.split('\n') :
        match = scopelistRE.search(line)
        if not match:
          newText += line + "\n"
        elif not listPrinted :
          listPrinted = True
          newText += sortedList + "\n"

    if numChanges == 0 :
      logger.info('No action taken')
      self.save_error_page()
      return
    else:
      page.text = newText.rstrip('\n')
      page.save(summary='{} insert into and sort alphabetical VI list by scope'.format(TASK_MESSAGE))

    #
    # removing candidates from candidate lists
    #
    
    candidates = ''
    for candpage in candpages:
      newText = ''
      page = pywikibot.Page(self.site, pageName + candpage )
      candidates = page.get(get_redirect=True)
      oldtext = candidates
      for line in candidates.split('\n') :
        keepLine = True
        # TODO: do we need this?
        uline = viutil.unescape( line )

        for remove in removeCandidates :
          #if string.find( uline.replace( ' ', '_' )  , remove.replace( ' ', '_' )  ) >= 0:
          if uline.lstrip("| ").replace( ' ', '_' ) == remove.replace( ' ', '_' ) :
            keepLine = False
            print("remove {}".format(line))
            print('  matched \'{}\' and \'{}\''.format(uline.replace( ' ', '_' ), remove.replace( ' ', '_' )))
            continue
  
        if keepLine :
          newText += line + "\n"

      page.text = emptyRE.sub( '', newText ).rstrip("\n")
      page.save(summary='{} remove processed nominations'.format(TASK_MESSAGE))

    #
    # Tag images
    #
    
    for image in tagImages:
      page = pywikibot.Page(self.site, 'File:' + image[0] )

      if page.exists() :
        #TODO already tagged maybe?
        text = page.get(get_redirect=True)
        oldtext = text
        text += "\n" + image[1]
        page.text = text
        page.save(summary='{} tag promoted Valued Image'.format(TASK_MESSAGE))
      else:
        self.error_page_content += '* In image taggin: [[{}]] does not exist\n'.format(image[0])
        logger.error('Oops, {} doesn\'t exist...'.format(image[0]))

    #    
    # Removed sorted images from Valued images/Recently promoted and dispatch them in galleries
    #
    
    self.dispatchRecentlyPromoted()

    #    
    # Add newly promoted images in Valued images/Recently promoted
    #
    
    self.populateRecentlyPromoted(tagImages)
    
    #
    # User notifications
    #
    
    for key in userNote:
      logger.info('notifying user {}'.format(key))

      page = pywikibot.Page(self.site, "User talk:" + key )

      if page.exists() :
        text = page.get(get_redirect=True)
        oldtext = text
      else :
        oldtext = ''
        text = 'Welcome to commons ' + key + ". What better way than starting off with a Valued Image promotion could there be? :-) --~~~~\n\n"
  
      text = text + "\n==Valued Image Promotion==\n" + userNote[key]
      page.text = text
      page.save(summary='{} notify user of promoted Valued Image(s)'.format(TASK_MESSAGE))

    #
    # Tag images in scope galleries
    #
    
    for entry in scopeList:
      tinySuccess = False

      # is there a link in the scope line?
      scrubbed = self.scrubscope(entry[1])
      link = linkRE.search(entry[1])
      if link:
        page = pywikibot.Page(self.site, link.group(1) )
      else:
        page = pywikibot.Page(self.site, scrubbed )

      if page.exists():	
        try:
          text = page.get(get_redirect=True)
          newText = ''
          for line in text.split('\n') :
            gallery = galleryRE.search( line )
            if gallery != None :
              if gallery.group(2).replace( ' ', '_' ) == entry[0].replace( ' ', '_' ) :
                newText += "{}|{{{{VI-tiny}}}} {}\n".format(line.split('|')[0], '|'.join(line.split('|')[1:]))
                tinySuccess = True
                logger.debug("success! " + scrubbed)
              else :
                newText += line + "\n"
            else :
              newText += line + "\n"
          page.text = newText.rstrip('\n')
          page.save(summary='{} tag images in galleries'.format(TASK_MESSAGE))
        except Exception as e:
          logger.error("exception in gallery tagging: {}".format(e))
      else:
        logger.error('Gallery {} does not exist'.format(page))

      if not tinySuccess :
        page = pywikibot.Page(self.site, pageName + "/tag_galleries" )
        if page.exists() :
          text = page.get(get_redirect=True)
          oldtext = text
        else :
          oldtext = ''
          text = "add <nowiki>{{VI-tiny}}</nowiki> at the gallery that matches the scope best and then remove the entry from this list\n\n"

        text = text + '\n*[[:File:{}|{}]]'.format( entry[0], scrubbed)
        page.text = text
        page.save(summary='{} tag images in galleries'.format(TASK_MESSAGE))
    # done!
    

  def dispatchRecentlyPromoted(self):
    """
      Takes sorted images from [[Commons:Valued images/Recently promoted]] and places them in [[Commons:Valued images by topic]]
      
      Arguments :
    """
    
    # Set the edit summary message
    logger.info('Adding recently categorized VIs to the VI galleries')
    
    recentPage = pywikibot.Page(self.site, 'Commons:Valued images/Recently promoted')
    galleryPrefix = 'Commons:Valued images by topic/'
    
    recentOldText = ""
    recentNewText = ""
    
    try:
      recentOldText = recentPage.get(get_redirect=True)
    except pywikibot.NoPage:
      logger.debug('Page {} does not exist; skipping.'.format(recentPage.aslink()))
      return
    except pywikibot.IsRedirectPage:
      logger.debug('Page {} is a redirect; skipping.'.format(recentPage.aslink()))
    
    #The structure recording the needed moves
    moveMap = {}
    
    #Find beginning of the gallery
    inGallery = False
    for line in recentOldText.split('\n'):
      if not inGallery:
        if line == '<gallery>':
          inGallery=True
          recentNewText += line + '\n'
          continue
        else:
          recentNewText += line + '\n'
      else:
        if line == '</gallery>':
          inGallery=False
          recentNewText += line + '\n'
          continue
        else:
          #Here we process an image
          firstPipePosition = line.find('|')
          fileName = line[0:firstPipePosition]
          caption = line[firstPipePosition + 1:]
          if caption.startswith('{{VICbotMove|'):
            #The VI is categorized already
            firstPipe = caption.find('|')
            lastPipe = caption.rfind('|')
            endOfTemplate = caption.rfind('}}')
            scope = caption[firstPipe+1:lastPipe]
            subpage = caption[lastPipe+1:endOfTemplate]
            if subpage not in list(moveMap.keys()):
              moveMap[subpage] = []
            moveMap[subpage].append((fileName, scope))
          else:
            #The VI is not categorized
            recentNewText += line + '\n'
    
    #Add pictures in galleries
    for subpage in moveMap.keys():
      galleryPage = pywikibot.Page(self.site, galleryPrefix + subpage)
      try:
        currentGalleryText = galleryPage.get(get_redirect=True)
      except pywikibot.NoPage:
        logger.warning('Page {} does not exist; skipping.'.format(galleryPage.aslink()))
        self.error_page_content += '* In gallery population: {} does not exist\n'.format(gallerypage.aslink())
        logger.debug("Skipped lines:")
        for pair in moveMap[subpage]:
          logger.debug(pair[0] + '|' + pair[1])
        continue
      except pywikibot.IsRedirectPage:
        logger.warning('Page {} is a redirect; skipping.'.format(galleryPage.aslink()))
        logger.debug("Skipped lines:")
        for pair in moveMap[subpage]:
          logger.debug(pair[0] + '|' + pair[1])
        continue
      endOfGal = currentGalleryText.rfind('\n</gallery>')
      if endOfGal < 0:
        logger.error('Gallery on page {} is malformed; skipping.'.format(galleryPage.aslink()))
        self.error_page_content += '* In gallery population: {} has a malformed gallery\n'.format(gallerypage.aslink())
        logger.warning("Skipped lines:")
        for pair in moveMap[subpage]:
          logger.warning(pair[0] + '|' + pair[1])
        continue
      newGalleryText = currentGalleryText[:endOfGal]
      for pair in moveMap[subpage]:
        if not pair[0] in currentGalleryText:
          newGalleryText += '\n' + pair[0] + '|' + pair[1]
      newGalleryText += currentGalleryText[endOfGal:]
      try:
        galleryPage.text = newGalleryText
        galleryPage.save(summary='{} add recently categorized [[COM:VI|valued images]] to the [[:Category:Galleries of valued images|VI galleries]]'.format(TASK_MESSAGE))
      except pywikibot.LockedPage:
        logger.warning('Page {} is locked; skipping.'.format(galleryPage.aslink()))
      except pywikibot.EditConflict:
        logger.warning('Skipping {} because of edit conflict'.format(galleryPage.title()))
      except pywikibot.SpamfilterError as error:
        logger.warning('Cannot change {} because of spam blacklist entry {}'.format(galleryPage.title(), error.url))
    
    #update the "Recently promoted" page
    recentNewText = recentNewText.rstrip()
    try:
      recentPage.text = recentNewText
      recentPage.save(summary='{} add recently categorized [[COM:VI|valued images]] to the [[:Category:Galleries of valued images|VI galleries]]'.format(TASK_MESSAGE))
    except pywikibot.LockedPage:
      logger.warning('Page {} is locked; skipping.'.format(recentPage.aslink()))
    except pywikibot.EditConflict:
      logger.warning('Skipping {} because of edit conflict'.format(recentPage.title()))
    except pywikibot.SpamfilterError as error:
      logger.warning('Cannot change {} because of spam blacklist entry {}'.format(recentPage.title(), error.url))
      self.error_page_content += '* In "recently promoted": couldn\'t save because of a spam blacklist entry\n'.format(gallerypage.aslink())

  def populateRecentlyPromoted(self, tagImages):
    """
      Adds the newly promoted VIs in [[Commons:Valued images/Recently promoted]]
      
      Arguments :
      tagImages   list constructed in the main program
    """
    recentPage = pywikibot.Page(self.site, "Commons:Valued images/Recently promoted")
      
    try:
      currentOutputText = recentPage.get(get_redirect=True)
    except pywikibot.NoPage:
      logger.warning('Page {} does not exist; skipping.'.format(page.aslink()))
      return
    except pywikibot.IsRedirectPage:
      logger.warning('Page {} is a redirect; skipping.'.format(page.aslink()))
      return
    except:
      logger.exception("An unhandled exception occured:")
      return
    
    endOfGal = currentOutputText.rfind('\n</gallery>')
    if endOfGal < 0:
      logger.error('Gallery on page {} is malformed; skipping.'.format(outputPage.aslink()))
      self.error_page_content += '* In gallery population: {} has a malformed gallery\n'.format(gallerypage.aslink())
    else:
      newOutputText = currentOutputText[:endOfGal]
      for image in tagImages:
        newOutputText += "\n" + image[2]
      newOutputText += currentOutputText[endOfGal:]
        
    try:
      recentPage.text = newOutputText
      recentPage.save(summary='{} preparing newly promoted [[COM:VI|Valued Images]] for sorting'.format(TASK_MESSAGE))
    except pywikibot.LockedPage:
      logger.warning('Page {} is locked; skipping.'.format(outputPage.aslink()))
    except pywikibot.EditConflict:
      logger.warning('Skipping {} because of edit conflict'.format(outputPage.title()))
    except pywikibot.SpamfilterError as error:
      logger.warning('Cannot change {} because of spam blacklist entry {}'.format(outputPage.title(), error.url))


  def save_error_page(self):
    error_page = pywikibot.Page(self.site, ERROR_PAGE)
    error_page.text = self.error_page_content
    error_page.save(summary='{} report task errors')


def main():
  pywikibot.handle_args()
  bot = VICbot()
  bot.run()

if __name__ == "__main__":
  try:
    main()
  finally:
    pywikibot.stopme()
